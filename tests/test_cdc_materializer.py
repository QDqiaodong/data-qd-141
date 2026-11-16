"""Tests for the local SQLite CDC history materializer."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from cdc_materializer import Materializer, BatchConflictError, REJECTION_REASONS
from cdc_materializer.timeutil import parse_valid_time, TimeParseError

ROOT = Path(__file__).resolve().parents[1]


def rec(event_id, op, source="s1", entity="e1", revision=1,
        valid_time="2026-01-01T00:00:00+00:00", value=None, target=None):
    out = {"event_id": event_id, "source": source, "entity": entity,
           "revision": revision, "op": op, "valid_time": valid_time}
    if op == "upsert":
        out["value"] = value
    else:
        if target is not None:
            out["target_event_id"] = target
    return out


def upsert(event_id, value, **kw):
    return rec(event_id, "upsert", value=value, **kw)


def retract(event_id, target, **kw):
    return rec(event_id, "retract", target=target, **kw)


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cdc.db")
        self.mat = Materializer(self.db)

    def tearDown(self):
        self.mat.close()
        self.tmp.cleanup()

    def ingest(self, batch_id, records):
        return self.mat.ingest_batch(batch_id, records)

    def value(self, source, entity, as_of, known_through=None):
        q = self.mat.query_materialized([source], [entity], as_of, known_through)
        (row,) = q["results"]
        return row

    def assertRejected(self, result, event_id, code):
        hits = [r for r in result["rejected"] if r["event_id"] == event_id]
        self.assertEqual(len(hits), 1, f"{event_id} not rejected once")
        self.assertEqual(hits[0]["reason_code"], code)
        # Reason codes are the stable contract; details carry specifics.
        self.assertTrue(hits[0]["reason_detail"])
        return hits[0]


class BasicAcceptance(BaseCase):
    def test_accept_and_query_basic_lifecycle(self):
        self.ingest("b1", [upsert("e1", {"v": 1}, revision=1)])
        self.assertEqual(self.mat.head_sequence(), 1)
        row = self.value("s1", "e1", "2026-02-01T00:00:00+00:00")
        self.assertEqual(row["state"], "present")
        self.assertEqual(row["value"], {"v": 1})
        self.assertEqual(row["event_id"], "e1")
        self.assertEqual(row["event_knowledge_seq"], 1)

        # Before the valid-time start the fact is empty.
        early = self.value("s1", "e1", "2025-12-31T23:59:59+00:00")
        self.assertEqual(early["state"], "empty")
        self.assertIsNone(early["value"])
        self.assertIsNone(early["event_id"])

        # Half-open boundary: exactly at valid_from the value is visible.
        boundary = self.value("s1", "e1", "2026-01-01T00:00:00+00:00")
        self.assertEqual(boundary["state"], "present")

    def test_valid_time_date_string_and_offset_normalization(self):
        self.assertEqual(
            parse_valid_time("2026-03-01"),
            "2026-03-01T00:00:00.000000+00:00")
        self.assertEqual(
            parse_valid_time("2026-03-01T08:00:00+08:00"),
            "2026-03-01T00:00:00.000000+00:00")
        self.assertEqual(
            parse_valid_time("2026-03-01T00:00:00Z"),
            "2026-03-01T00:00:00.000000+00:00")
        for bad in ["", "not-a-time", "2026-13-01", "2026-03-01T00:00:00",
                    "2026-02-30", 12345, None]:
            with self.assertRaises(TimeParseError):
                parse_valid_time(bad)

    def test_half_open_valid_intervals_and_as_of_slicing(self):
        self.ingest("b1", [
            upsert("e1", "jan", revision=1,
                   valid_time="2026-01-01T00:00:00+00:00"),
            upsert("e2", "feb", revision=2,
                   valid_time="2026-02-01T00:00:00+00:00"),
        ])
        jan = self.value("s1", "e1", "2026-01-15T00:00:00+00:00")
        self.assertEqual(jan["value"], "jan")
        # Half-open boundary: at exactly the next start the old interval ended.
        at_feb = self.value("s1", "e1", "2026-02-01T00:00:00+00:00")
        self.assertEqual(at_feb["event_id"], "e2")
        self.assertEqual(at_feb["value"], "feb")
        # And jan is still the answer for any instant strictly before Feb.
        self.assertEqual(
            self.value("s1", "e1", "2026-01-31T23:59:59+00:00")["event_id"],
            "e1")


class RevisionAndRetractSemantics(BaseCase):
    def test_higher_revision_same_valid_start_replaces(self):
        self.ingest("b1", [upsert("e-old", {"v": 1}, revision=1)])
        self.ingest("b2", [upsert("e-new", {"v": 2}, revision=2)])
        # Knowledge seq 1 keeps returning the original fact forever.
        old_cutoff = self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                                known_through=1)
        self.assertEqual(old_cutoff["value"], {"v": 1})
        self.assertEqual(old_cutoff["event_id"], "e-old")
        # New cutoff only sees the replacement.
        new_cutoff = self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                                known_through=2)
        self.assertEqual(new_cutoff["value"], {"v": 2})
        self.assertEqual(new_cutoff["event_id"], "e-new")

    def test_acceptance_scenario_retract_early_then_higher_revision(self):
        """The headline acceptance case from the spec."""
        self.ingest("b1", [upsert("e-fact", {"v": "original"}, revision=1)])
        self.ingest("b2", [
            retract("e-retract", "e-fact", revision=2),
            upsert("e-replacement", {"v": "corrected"}, revision=3),
        ])
        self.assertEqual(self.mat.head_sequence(), 2)

        # Old knowledge cutoff still returns the original fact byte-for-byte.
        old = self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                         known_through=1)
        self.assertEqual(old["state"], "present")
        self.assertEqual(old["value"], {"v": "original"})
        self.assertEqual(old["event_id"], "e-fact")

        # New cutoff: retraction closed the original, higher revision wins.
        new = self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                         known_through=2)
        self.assertEqual(new["state"], "present")
        self.assertEqual(new["value"], {"v": "corrected"})
        self.assertEqual(new["event_id"], "e-replacement")
        self.assertIsNone(new["retracted_by_event_id"])

    def test_retract_makes_value_empty_with_provenance(self):
        self.ingest("b1", [upsert("e-fact", {"v": 1}, revision=1)])
        self.ingest("b2", [retract("e-r", "e-fact", revision=2)])
        empty = self.value("s1", "e1", "2026-06-01T00:00:00+00:00")
        self.assertEqual(empty["state"], "empty")
        self.assertIsNone(empty["value"])
        self.assertIsNone(empty["event_id"])
        self.assertEqual(empty["retracted_by_event_id"], "e-r")
        # Prior cutoff unaffected by the later retraction.
        old = self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                         known_through=1)
        self.assertEqual(old["state"], "present")
        self.assertEqual(old["value"], {"v": 1})

    def test_retract_only_closes_from_target_valid_time(self):
        self.ingest("b1", [
            upsert("e-a", "A", revision=1,
                   valid_time="2026-01-01T00:00:00+00:00"),
            upsert("e-b", "B", revision=1, entity="e2",
                   valid_time="2026-03-01T00:00:00+00:00"),
        ])
        self.ingest("b2", [
            retract("r-a", "e-a", revision=2,
                    valid_time="2026-05-01T00:00:00+00:00"),
        ])
        # The retraction starts at the *target's* valid time, so e1 is empty
        # even though the retract event itself is dated later.
        self.assertEqual(
            self.value("s1", "e1", "2026-02-01T00:00:00+00:00")["state"],
            "empty")
        self.assertEqual(
            self.value("s1", "e2", "2026-04-01T00:00:00+00:00")["value"], "B")

    def test_retract_chain(self):
        self.ingest("b1", [upsert("f1", 1, revision=1)])
        self.ingest("b2", [
            retract("r1", "f1", revision=2),
            upsert("f2", 2, revision=3),
        ])
        self.ingest("b3", [
            retract("r2", "f2", revision=4),
            upsert("f3", 3, revision=5),
        ])
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00")["value"], 3)
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                       known_through=2)["value"], 2)
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00",
                       known_through=1)["value"], 1)
        # Every earlier answer is byte-stable under later replays.
        for cutoff in (1, 2, 3):
            q = self.mat.query_materialized(
                ["s1"], ["e1"], "2026-06-01T00:00:00+00:00", cutoff)
            again = self.mat.query_materialized(
                ["s1"], ["e1"], "2026-06-01T00:00:00+00:00", cutoff)
            self.assertEqual(
                json.dumps(q, sort_keys=True), json.dumps(again, sort_keys=True))

    def test_unknown_entity_returns_empty_entry(self):
        self.ingest("b1", [upsert("e1", 1)])
        q = self.mat.query_materialized(
            ["s1", "s2"], ["e1", "e9"], "2026-06-01T00:00:00+00:00")
        keys = {(r["source"], r["entity"]): r for r in q["results"]}
        self.assertEqual(set(keys), {("s1", "e1"), ("s1", "e9"),
                                     ("s2", "e1"), ("s2", "e9")})
        self.assertEqual(keys[("s1", "e1")]["state"], "present")
        for pair in (("s1", "e9"), ("s2", "e1"), ("s2", "e9")):
            self.assertEqual(keys[pair]["state"], "empty")


class IdempotencyAndOrdering(BaseCase):
    def test_duplicate_batch_is_idempotent(self):
        records = [upsert("e1", 1, revision=1),
                   upsert("e2", 2, entity="e2", revision=1)]
        first = self.ingest("b1", records)
        second = self.ingest("b1", json.loads(json.dumps(records)))
        self.assertEqual(first, second)
        self.assertEqual(self.mat.head_sequence(), 1)
        self.assertEqual(self.mat.get_batch("b1")["accepted_count"], 2)

    def test_duplicate_batch_with_different_bytes_conflicts(self):
        self.ingest("b1", [upsert("e1", 1, revision=1)])
        with self.assertRaises(BatchConflictError):
            self.ingest("b1", [upsert("e1", 2, revision=1)])
        with self.assertRaises(BatchConflictError):
            self.ingest("b1", [])
        # Conflicting delivery changed nothing.
        self.assertEqual(self.mat.head_sequence(), 1)

    def test_out_of_order_batches_still_materialize_correctly(self):
        # Higher revision arrives before the lower one (which then conflicts).
        self.ingest("b-late-higher", [
            upsert("e2", "high", revision=2,
                   valid_time="2026-01-01T00:00:00+00:00")])
        result = self.ingest("b-early-lower", [
            upsert("e1", "low", revision=1,
                   valid_time="2025-12-01T00:00:00+00:00")])
        self.assertEqual(result["accepted_count"], 1)
        # Re-delivering the already-held revision 2 on the same triple loses.
        result2 = self.ingest("b-conflict", [upsert("e3", "dup", revision=2)])
        self.assertRejected(result2, "e3", "revision_conflict")
        # The materialized model: at a cutoff, take the greatest valid start
        # not later than as_of among the then-known facts.  A fact arriving
        # late with an earlier start therefore never back-fills a span already
        # covered by a known later start; it only answers earlier as_of points.
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00")["value"],
            "high")
        before = self.value("s1", "e1", "2025-12-15T00:00:00+00:00")
        self.assertEqual(before["value"], "low")
        # Half-open: exactly at rev2's start, the later valid start wins.
        boundary = self.value("s1", "e1", "2026-01-01T00:00:00+00:00")
        self.assertEqual(boundary["value"], "high")
        # At the knowledge cutoff before rev1's batch existed, nothing leaks.
        self.assertEqual(
            self.value("s1", "e1", "2025-12-15T00:00:00+00:00",
                       known_through=1)["state"], "empty")
        # And at the old cutoff, rev2 alone still answers January onward.
        self.assertEqual(
            self.value("s1", "e1", "2026-02-01T00:00:00+00:00",
                       known_through=1)["value"], "high")

    def test_knowledge_sequences_are_continuous_and_retry_safe(self):
        for i in range(1, 6):
            self.ingest(f"b{i}", [upsert(f"e{i}", i, revision=1,
                                         entity=f"ent{i}")])
        self.assertEqual(self.mat.head_sequence(), 5)
        # Replay each: no extra sequence, same verdicts.
        seq_seen = []
        for i in range(1, 6):
            replay = self.ingest(f"b{i}", [upsert(f"e{i}", i, revision=1,
                                                  entity=f"ent{i}")])
            seq_seen.append(replay["knowledge_seq"])
        # Replayed batches report the sequence they originally allocated.
        self.assertEqual(seq_seen, [1, 2, 3, 4, 5])
        self.assertEqual(self.mat.head_sequence(), 5)
        seqs = [r["event_knowledge_seq"] for i in range(1, 6)
                for r in self.mat.query_materialized(
                    ["s1"], [f"ent{i}"],
                    "2026-06-01T00:00:00+00:00")["results"]]
        self.assertEqual(sorted(seqs), [1, 2, 3, 4, 5])

    def test_batch_with_only_rejections_allocates_no_sequence(self):
        result = self.ingest("b1", [
            upsert("e1", 1, revision=-1),
            {"event_id": "x", "op": "upsert"},
        ])
        self.assertEqual(result["accepted_count"], 0)
        self.assertEqual(result["rejected_count"], 2)
        self.assertIsNone(result["knowledge_seq"])
        self.assertEqual(self.mat.head_sequence(), 0)
        # The rejected event_ids are still queryable.
        self.assertEqual(self.mat.lookup_event("e1")["status"], "rejected")
        self.assertEqual(self.mat.lookup_event("x")["status"], "rejected")


class PerRecordRejection(BaseCase):
    def test_one_bad_record_does_not_drop_the_batch(self):
        good = upsert("good", 1, revision=1)
        bad_time = upsert("bad-time", 2, revision=2, valid_time="nonsense")
        bad_rev = upsert("bad-rev", 3, revision="oops")
        result = self.ingest("b1", [good, bad_time, bad_rev, "scalar",
                                    {"event_id": "z", "bogus": 1}])
        self.assertEqual(result["accepted_count"], 1)
        self.assertEqual(result["rejected_count"], 4)
        self.assertEqual(result["accepted"], ["good"])
        self.assertRejected(result, "bad-time", "invalid_valid_time")
        self.assertRejected(result, "bad-rev", "invalid_revision")
        scalar = next(r for r in result["rejected"]
                      if r["reason_code"] == "malformed_record"
                      and r["event_id"] is None)
        self.assertEqual(scalar["record_index"], 3)
        self.assertRejected(result, "z", "unknown_field")
        # Good record is live and got sequence 1.
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00")["event_id"],
            "good")

    def test_structural_reason_catalogue(self):
        cases = [
            ({"event_id": "a1", "source": "s", "entity": "e",
              "revision": 1, "op": "upsert", "valid_time": "2026-01-01"},
             "malformed_record"),  # upsert without a usable value field
            ({"event_id": "", "source": "s", "entity": "e", "revision": 1,
              "op": "upsert", "valid_time": "2026-01-01", "value": 1},
             "invalid_event_id"),
            ({"event_id": "a2", "source": "", "entity": "e", "revision": 1,
              "op": "upsert", "valid_time": "2026-01-01", "value": 1},
             "invalid_source"),
            ({"event_id": "a3", "source": "s", "entity": "e", "revision": 1.5,
              "op": "upsert", "valid_time": "2026-01-01", "value": 1},
             "invalid_revision"),
            ({"event_id": "a4", "source": "s", "entity": "e", "revision": 1,
              "op": "delete", "valid_time": "2026-01-01", "value": 1},
             "invalid_operation"),
            ({"event_id": "a5", "source": "s", "entity": "e", "revision": 1,
              "op": "retract", "valid_time": "2026-01-01"},
             "retract_missing_target"),
            ({"event_id": "a6", "source": "s", "entity": "e", "revision": 1,
              "op": "retract", "valid_time": "2026-01-01",
              "target_event_id": "x", "value": 1},
             "retract_has_value"),
        ]
        result = self.ingest("b1", [c[0] for c in cases])
        for record, code in cases:
            if not record["event_id"]:
                # An empty id cannot be a lookup key; verify by record index.
                null_hits = [r for r in result["rejected"]
                             if r["event_id"] is None]
                self.assertTrue(null_hits)
                self.assertEqual(null_hits[0]["reason_code"], code)
            else:
                self.assertRejected(result, record["event_id"], code)

    def test_duplicate_event_id_in_batch_rejected_individually(self):
        result = self.ingest("b1", [
            upsert("dup", 1, revision=1),
            upsert("dup", 2, revision=2),
            upsert("other", 3, revision=3),
        ])
        self.assertEqual(result["accepted_count"], 1)
        self.assertIn("dup", [r["event_id"] for r in result["rejected"]])
        for rj in result["rejected"]:
            if rj["event_id"] == "dup":
                self.assertEqual(rj["reason_code"],
                                 "duplicate_event_id_in_batch")
        self.assertEqual(result["accepted"], ["other"])

    def test_reused_event_id_after_prior_acceptance_or_rejection(self):
        self.ingest("b1", [upsert("e1", 1, revision=1),
                           upsert("bad", 2, valid_time="nope", revision=2)])
        result = self.ingest("b2", [
            upsert("e1", 9, revision=9),
            upsert("bad", 9, revision=9),
        ])
        self.assertEqual(result["accepted_count"], 0)
        for rj in result["rejected"]:
            self.assertEqual(rj["reason_code"], "event_id_already_recorded")

    def test_revision_conflict_detail_names_holder(self):
        self.ingest("b1", [upsert("holder", 1, revision=7)])
        result = self.ingest("b2", [upsert("rival", 2, revision=7)])
        rj = self.assertRejected(result, "rival", "revision_conflict")
        self.assertIn("holder", rj["reason_detail"])

    def test_retract_rejection_reasons(self):
        self.ingest("b1", [
            upsert("f1", 1, revision=1),
            upsert("f1b", 1, revision=1, entity="e2"),
        ])
        self.ingest("b2", [retract("r1", "f1", revision=2)])
        result = self.ingest("b3", [
            retract("r-unknown", "ghost", revision=3),
            retract("r-inactive", "f1", revision=4),
            retract("r-cross", "f1b", revision=5),
        ])
        self.assertRejected(result, "r-unknown", "unknown_retract_target")
        self.assertRejected(result, "r-inactive", "retract_target_inactive")
        self.assertRejected(result, "r-cross", "unknown_retract_target")

    def test_retract_cycle_rejected_per_record(self):
        self.ingest("b0", [upsert("f1", 1, revision=1)])
        result = self.ingest("b1", [
            retract("c1", "c2", revision=2),
            retract("c2", "c3", revision=3),
            retract("c3", "c1", revision=4),
            upsert("survivor", 42, revision=5),
        ])
        self.assertEqual(result["accepted"], ["survivor"])
        for eid in ("c1", "c2", "c3"):
            self.assertRejected(result, eid, "retract_cycle")
        # b0 allocated seq 1; the surviving upsert still gets the next one,
        # while every cyclic retract was refused.
        self.assertEqual(self.mat.head_sequence(), 2)
        self.assertEqual(
            self.mat.lookup_event("c1")["reason_code"], "retract_cycle")

    def test_same_batch_retract_of_same_batch_upsert(self):
        result = self.ingest("b1", [
            upsert("f1", 1, revision=1),
            retract("r1", "f1", revision=2),
        ])
        self.assertEqual(result["accepted_count"], 2)
        row = self.value("s1", "e1", "2026-06-01T00:00:00+00:00")
        self.assertEqual(row["state"], "empty")
        self.assertEqual(row["retracted_by_event_id"], "r1")

    def test_same_batch_retract_of_superseded_target_is_rejected(self):
        result = self.ingest("b1", [
            upsert("f-old", 1, revision=1),
            upsert("f-new", 2, revision=2),       # same valid start
            retract("r-late", "f-old", revision=3),
        ])
        self.assertEqual(sorted(result["accepted"]), ["f-new", "f-old"])
        self.assertRejected(result, "r-late", "retract_target_inactive")
        # The replacement still wins at the head cutoff.
        self.assertEqual(
            self.value("s1", "e1", "2026-06-01T00:00:00+00:00")["value"], 2)

    def test_duplicate_retract_target_in_one_batch(self):
        result = self.ingest("b1", [
            upsert("f1", 1, revision=1),
            retract("r1", "f1", revision=2),
            retract("r2", "f1", revision=3),
        ])
        self.assertEqual(sorted(result["accepted"]), ["f1", "r1"])
        self.assertRejected(result, "r2", "retract_target_inactive")
        row = self.value("s1", "e1", "2026-06-01T00:00:00+00:00")
        self.assertEqual(row["state"], "empty")
        self.assertEqual(row["retracted_by_event_id"], "r1")

    def test_retract_of_surviving_same_start_upsert_is_accepted(self):
        # Two upserts at one start: the later record wins.  A retract aimed at
        # the winner must still be accepted; only the earlier holder is dead.
        result = self.ingest("b1", [
            upsert("f-old", 1, revision=1),
            upsert("f-new", 2, revision=2),
            retract("r-dead", "f-old", revision=3),
            retract("r-live", "f-new", revision=4),
        ])
        self.assertRejected(result, "r-dead", "retract_target_inactive")
        self.assertEqual(sorted(result["accepted"]),
                         ["f-new", "f-old", "r-live"])
        row = self.value("s1", "e1", "2026-06-01T00:00:00+00:00")
        self.assertEqual(row["state"], "empty")
        self.assertEqual(row["retracted_by_event_id"], "r-live")


class LookupsAndStability(BaseCase):
    def test_event_and_revision_lookups(self):
        self.ingest("b1", [
            upsert("ok", 1, revision=1),
            upsert("bad", 2, revision="x"),
        ])
        verdict = self.mat.lookup_event("ok")
        self.assertEqual(verdict["status"], "accepted")
        self.assertEqual(verdict["knowledge_seq"], 1)
        self.assertEqual(self.mat.lookup_event("bad")["status"], "rejected")
        self.assertEqual(self.mat.lookup_event("missing")["status"], "unknown")

        rev = self.mat.lookup_revision("s1", "e1", 1)
        self.assertEqual(rev["status"], "accepted")
        self.assertEqual(rev["event_id"], "ok")
        self.assertEqual(
            self.mat.lookup_revision("s1", "e1", 99)["status"], "unknown")

    def test_repeated_query_is_byte_stable(self):
        self.ingest("b1", [
            upsert("e1", {"z": 1, "a": 2}, revision=1),
            upsert("e2", [3, 1, 2], entity="e2", revision=1),
        ])
        payloads = set()
        for _ in range(5):
            q = self.mat.query_materialized(
                ["s1"], ["e1", "e2"], "2026-06-01T00:00:00+00:00")
            payloads.add(json.dumps(q, sort_keys=True,
                                    separators=(",", ":"), ensure_ascii=False))
        self.assertEqual(len(payloads), 1)

    def test_earlier_cutoff_immutable_under_later_revisions(self):
        self.ingest("b1", [upsert("e1", "v1", revision=1)])
        snapshot1 = json.dumps(self.mat.query_materialized(
            ["s1"], ["e1"], "2026-06-01T00:00:00+00:00", 1), sort_keys=True)
        for rev in range(2, 8):
            self.ingest(f"b{rev}", [upsert(f"e{rev}", f"v{rev}",
                                           revision=rev)])
        snapshot1_after = json.dumps(self.mat.query_materialized(
            ["s1"], ["e1"], "2026-06-01T00:00:00+00:00", 1), sort_keys=True)
        self.assertEqual(snapshot1, snapshot1_after)
        self.assertEqual(
            self.mat.query_materialized(
                ["s1"], ["e1"], "2026-06-01T00:00:00+00:00", 4)
            ["results"][0]["value"], "v4")


class TransactionsAndRecovery(BaseCase):
    def test_simulated_crash_leaves_no_partial_state(self):
        crashy = Materializer(self.db, simulate_crash_before_commit=True)
        with self.assertRaises(sqlite3.OperationalError):
            crashy.ingest_batch("b1", [
                upsert("e1", 1, revision=1),
                upsert("e2", 2, revision=2, entity="e2"),
                retract("r-bad", "ghost", revision=3),
            ])
        crashy.close()

        # Reopen: no batch, no events, no rejections, no facts, seq at zero.
        recovered = Materializer(self.db)
        self.assertIsNone(recovered.get_batch("b1"))
        self.assertEqual(recovered.head_sequence(), 0)
        self.assertEqual(recovered.lookup_event("e1")["status"], "unknown")
        with sqlite3.connect(self.db) as raw:
            for table in ("batches", "events", "rejections", "facts",
                          "entity_versions"):
                count = raw.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                self.assertEqual(count, 0, f"{table} survived a crash rollback")

        # Retry the same batch on the recovered store: fully accepted now.
        result = recovered.ingest_batch("b1", [
            upsert("e1", 1, revision=1),
            upsert("e2", 2, revision=2, entity="e2"),
        ])
        self.assertEqual(result["accepted_count"], 2)
        self.assertEqual(result["knowledge_seq"], 1)
        recovered.close()

    def test_reopen_restores_full_state(self):
        self.ingest("b1", [upsert("e1", {"v": 1}, revision=1)])
        self.ingest("b2", [retract("r1", "e1", revision=2)])
        self.mat.close()
        reopened = Materializer(self.db)
        self.assertEqual(reopened.head_sequence(), 2)
        self.assertEqual(
            reopened.lookup_event("r1")["status"], "accepted")
        row = reopened.query_materialized(
            ["s1"], ["e1"], "2026-06-01T00:00:00+00:00")["results"][0]
        self.assertEqual(row["state"], "empty")
        self.assertEqual(row["retracted_by_event_id"], "r1")
        old = reopened.query_materialized(
            ["s1"], ["e1"], "2026-06-01T00:00:00+00:00",
            known_through=1)["results"][0]
        self.assertEqual(old["value"], {"v": 1})
        reopened.close()
        self.mat = Materializer(self.db)  # keep tearDown happy

    def test_reader_never_sees_mixed_cutoff_during_commit(self):
        self.ingest("b0", [upsert("seed", 0, revision=1)])
        errors = []
        stop = threading.Event()

        def reader_loop():
            try:
                for _ in range(300):
                    q = self.mat.query_materialized(
                        ["s1"], ["e1"], "2026-06-01T00:00:00+00:00")
                    (row,) = q["results"]
                    # Value must always be exactly one coherent version.
                    if row["state"] == "present":
                        self.assertIn(row["event_knowledge_seq"],
                                      range(1, 12))
                    if stop.is_set():
                        break
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=reader_loop) for _ in range(3)]
        for t in threads:
            t.start()
        for rev in range(2, 12):
            self.ingest(f"b{rev}", [upsert(f"e{rev}", rev, revision=rev)])
        stop.set()
        for t in threads:
            t.join(timeout=10)
            self.assertFalse(t.is_alive())
        self.assertEqual(errors, [])
        final = self.value("s1", "e1", "2026-06-01T00:00:00+00:00")
        self.assertEqual(final["value"], 11)

    def test_concurrent_ingest_in_same_process_is_serialized(self):
        failures = []

        def worker(seq):
            try:
                self.mat.ingest_batch(
                    f"b{seq}",
                    [upsert(f"e{seq}", seq, revision=1,
                            entity=f"ent{seq}")])
            except Exception as exc:  # pragma: no cover
                failures.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        self.assertEqual(failures, [])
        self.assertEqual(self.mat.head_sequence(), 8)
        # No gaps in the knowledge sequence.
        with sqlite3.connect(self.db) as raw:
            seqs = [r[0] for r in raw.execute(
                "SELECT DISTINCT knowledge_seq FROM events ORDER BY 1")]
        self.assertEqual(seqs, list(range(1, 9)))


class RandomizedOracle(BaseCase):
    """Cross-checks interval splicing against an independent naive oracle."""

    def test_random_batches_match_oracle(self):
        import random
        rng = random.Random(20261116)
        base = "2026-01-01T00:00:00+00:00"
        months = [f"2026-{m:02d}-01T00:00:00+00:00" for m in range(1, 12)]
        # Accepted events retained by the oracle: list of batch groups, each
        # ordered like the engine applies them (upserts, then retractions).
        accepted = []
        live_ids: set[str] = set()
        head = 0

        def oracle_answer(as_of, cutoff):
            # Independent replay of the spec model.  State is keyed by event,
            # not by valid start: several events may share a start (revision
            # replacements), and a retraction must remove exactly its target
            # event rather than whichever event currently occupies the slot.
            live_events = {}   # event_id -> (valid_time, value)
            for group in accepted:
                for ev in group:
                    if ev["seq"] > cutoff:
                        continue
                    if ev["op"] == "upsert":
                        # Same-start replacement: the old holder at that start
                        # is superseded by the newer accepted event.
                        for other in list(live_events):
                            if live_events[other][0] == ev["valid_time"]:
                                del live_events[other]
                        live_events[ev["event_id"]] = (
                            ev["valid_time"], ev["value"])
                    else:
                        live_events.pop(ev["target_event_id"], None)
            candidates = [(vt, (eid, val))
                          for eid, (vt, val) in live_events.items()
                          if vt <= as_of]
            if not candidates:
                return None, None
            _, (event_id, value) = max(candidates, key=lambda item: item[0])
            return value, event_id

        event_seq = 0
        rev = 0
        for batch_index in range(24):
            records = []
            batch_accepted_before = {
                e["event_id"] for group in accepted for e in group}
            used_this_batch = set()
            for _ in range(rng.randint(1, 4)):
                event_seq += 1
                eid = f"ev{event_seq}"
                rev += 1
                vt = rng.choice(months)
                if live_ids and rng.random() < 0.35:
                    target = rng.choice(sorted(live_ids))
                    records.append(retract(
                        eid, target, revision=rev, valid_time=vt))
                else:
                    records.append(upsert(
                        eid, f"v{event_seq}", revision=rev, valid_time=vt))
                used_this_batch.add(eid)

            # Sprinkle in records the engine must reject.
            if rng.random() < 0.4:
                records.append(upsert(
                    f"bad{event_seq}", 1, revision="no", valid_time=base))
            if batch_accepted_before and rng.random() < 0.3:
                dup_id = next(iter(batch_accepted_before))
                records.append(upsert(dup_id, 999, revision=rev + 100,
                                      valid_time=base))

            result = self.ingest(f"batch-{batch_index}", records)
            for rj in result["rejected"]:
                self.assertTrue(rj["reason_code"] in REJECTION_REASONS)
            if result["accepted"]:
                head += 1  # one knowledge sequence per accepted batch
            group = []
            # Engine application order: upserts before retractions.
            ordered_ids = [i for i in result["accepted"]
                           if next(r for r in records
                                   if r["event_id"] == i)["op"] == "upsert"]
            ordered_ids += [i for i in result["accepted"]
                            if next(r for r in records
                                    if r["event_id"] == i)["op"] == "retract"]
            for ev_id in ordered_ids:
                rec = next(r for r in records if r["event_id"] == ev_id)
                entry = {"event_id": ev_id, "op": rec["op"],
                         "valid_time": parse_valid_time(rec["valid_time"]),
                         "seq": head, "value": rec.get("value"),
                         "target_event_id": rec.get("target_event_id")}
                group.append(entry)
                if rec["op"] == "upsert":
                    live_ids.add(ev_id)
                else:
                    live_ids.discard(rec["target_event_id"])
            if group:
                accepted.append(group)
            self.assertEqual(self.mat.head_sequence(), head)

            # Compare every cutoff/as-of combination with the naive oracle.
            for cutoff in range(0, head + 1):
                for as_of in months + ["2025-12-15T00:00:00+00:00"]:
                    row = self.value("s1", "e1", as_of,
                                     known_through=cutoff)
                    o_value, o_event = oracle_answer(
                        parse_valid_time(as_of), cutoff)
                    if o_value is None:
                        self.assertEqual(
                            row["state"], "empty",
                            f"leak at cutoff={cutoff} as_of={as_of}")
                    else:
                        self.assertEqual(row["state"], "present")
                        self.assertEqual(row["value"], o_value)
                        self.assertEqual(row["event_id"], o_event)


class CliSmoke(unittest.TestCase):
    def test_cli_ingest_query_lookup(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "cdc.db")
            batch = os.path.join(td, "batch.json")
            Path(batch).write_text(json.dumps([
                upsert("e1", {"v": 1}, revision=1),
                upsert("bad", 2, revision=-1),
            ]), encoding="utf-8")
            env = {**os.environ,
                   "PYTHONPATH": str(ROOT / "src")}

            def run(*args):
                return subprocess.run(
                    [sys.executable, "-m", "cdc_materializer.cli", *args],
                    capture_output=True, text=True, env=env, cwd=td)

            r = run("ingest", "--db", db, "--batch-id", "b1",
                    "--input", batch)
            self.assertEqual(r.returncode, 0, r.stderr)
            out = json.loads(r.stdout)
            self.assertEqual(out["accepted_count"], 1)
            self.assertEqual(out["rejected_count"], 1)

            r2 = run("ingest", "--db", db, "--batch-id", "b1",
                     "--input", batch)
            self.assertEqual(json.loads(r2.stdout), out)  # idempotent

            rq = run("query", "--db", db, "--sources", "s1",
                     "--entities", "e1", "--as-of",
                     "2026-06-01T00:00:00+00:00")
            self.assertEqual(rq.returncode, 0, rq.stderr)
            row = json.loads(rq.stdout)["results"][0]
            self.assertEqual(row["value"], {"v": 1})

            re_ = run("event", "--db", db, "--event-id", "bad")
            self.assertEqual(json.loads(re_.stdout)["reason_code"],
                             "invalid_revision")
            rh = run("head", "--db", db)
            self.assertEqual(json.loads(rh.stdout)["head_sequence"], 1)


if __name__ == "__main__":
    unittest.main()
