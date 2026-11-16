"""CDC ingest engine and bi-temporal interval materialization on SQLite.

Concurrency model
-----------------
A single writer connection owns all mutating work.  Every batch runs inside
one ``BEGIN IMMEDIATE`` transaction so that validation, the knowledge sequence,
event inserts and fact-interval re-splicing commit atomically (or roll back
together).  Readers open separate read-only connections and hold one snapshot
transaction for the whole query, so a batch committing mid-query can never
produce mixed-cutoff results.

Why facts are never rewritten
-----------------------------
Each fact row carries a knowledge interval ``[create_seq, close_seq)``.  New
revisions/retractions *close* old rows (append-only) and insert new rows, which
keeps every earlier ``known_through`` answer immutable while the latest cutoff
sees only the new state.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from .schema import SCHEMA_SQL, SCHEMA_VERSION
from .timeutil import TimeParseError, parse_valid_time

# ---------------------------------------------------------------------------
# Stable rejection reason catalogue.  Codes are part of the public contract:
# never rename them, only append new ones.
# ---------------------------------------------------------------------------
REJECTION_REASONS = {
    "malformed_record": "record is not a JSON object",
    "unknown_field": "record contains an undefined field",
    "invalid_event_id": "event_id must be a non-empty string",
    "invalid_source": "source must be a non-empty string",
    "invalid_entity": "entity must be a non-empty string",
    "invalid_revision": "revision must be a non-negative integer",
    "invalid_operation": "op must be 'upsert' or 'retract'",
    "invalid_valid_time": "valid_time cannot be resolved to an instant",
    "retract_missing_target": "retract requires a non-empty target_event_id",
    "retract_has_value": "retract records must not carry a value",
    "event_id_already_recorded": "event_id was already accepted or rejected",
    "duplicate_event_id_in_batch": "event_id appears more than once in the batch",
    "revision_conflict": "an event with the same source/entity/revision already exists",
    "unknown_retract_target": "retract target event is unknown or belongs to another entity",
    "retract_target_inactive": "retract target fact is already retracted or superseded",
    "retract_cycle": "retract chain forms a cycle",
}

_ALLOWED_FIELDS = frozenset(
    {"event_id", "source", "entity", "revision", "op", "valid_time",
     "value", "target_event_id"}
)
_OPS = frozenset({"upsert", "retract"})


class BatchConflictError(Exception):
    """A known batch_id was re-delivered with different canonical bytes."""


def _is_int(value: Any) -> bool:
    # bool is a subclass of int but is not an acceptable revision.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def canonical_json(value: Any) -> str:
    """Deterministic JSON encoding used for hashes, payloads and byte stability."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


class Materializer:
    """Owns one SQLite database file containing the CDC history store."""

    def __init__(self, path: str, *, simulate_crash_before_commit: bool = False):
        self.path = os.path.abspath(path)
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # check_same_thread=False is safe because every write is guarded by
        # self._write_lock; readers never share this connection.
        self._writer = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self._writer.row_factory = sqlite3.Row
        self._write_lock = threading.RLock()
        self._simulate_crash = simulate_crash_before_commit
        self._initialize()

    # ------------------------------------------------------------------ setup
    def _initialize(self) -> None:
        with self._write_lock:
            self._writer.executescript(SCHEMA_SQL)
            row = self._writer.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                self._writer.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(row["value"]) != SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported schema version {row['value']!r}; "
                    f"expected {SCHEMA_VERSION}"
                )

    def close(self) -> None:
        with self._write_lock:
            self._writer.close()

    def __enter__(self) -> "Materializer":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- inspection
    def head_sequence(self) -> int:
        """Latest allocated knowledge sequence (0 when nothing accepted yet)."""
        with self._write_lock:
            row = self._writer.execute(
                "SELECT COALESCE(MAX(knowledge_seq), 0) AS h FROM events"
            ).fetchone()
            return int(row["h"])

    def get_batch(self, batch_id: str) -> dict | None:
        with self._write_lock:
            row = self._writer.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if row is None:
                return None
            return self._rebuild_result(batch_id, row)

    def list_batches(self) -> list[dict]:
        with self._write_lock:
            rows = self._writer.execute(
                "SELECT batch_id FROM batches ORDER BY rowid"
            ).fetchall()
            return [self._rebuild_result(r["batch_id"]) for r in rows]

    def lookup_event(self, event_id: str) -> dict:
        """Verdict for an event_id: accepted / rejected / unknown."""
        if not _is_nonempty_str(event_id):
            raise ValueError("event_id must be a non-empty string")
        with self._write_lock:
            ev = self._writer.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if ev is not None:
                return self._event_verdict(ev)
            rj = self._writer.execute(
                "SELECT * FROM rejections WHERE event_id=? ORDER BY id LIMIT 1",
                (event_id,),
            ).fetchone()
            if rj is not None:
                return self._rejection_verdict(rj)
            return {"event_id": event_id, "status": "unknown"}

    def lookup_revision(self, source: str, entity: str, revision: int) -> dict:
        """Verdict for a (source, entity, revision) triple."""
        if not _is_nonempty_str(source) or not _is_nonempty_str(entity):
            raise ValueError("source and entity must be non-empty strings")
        if not _is_int(revision) or revision < 0:
            raise ValueError("revision must be a non-negative integer")
        with self._write_lock:
            ev = self._writer.execute(
                "SELECT * FROM events WHERE source=? AND entity=? AND revision=?",
                (source, entity, revision),
            ).fetchone()
            if ev is not None:
                return self._event_verdict(ev)
            rj = self._writer.execute(
                "SELECT * FROM rejections WHERE source=? AND entity=? AND revision=? "
                "ORDER BY id LIMIT 1",
                (source, entity, revision),
            ).fetchone()
            if rj is not None:
                return self._rejection_verdict(rj)
            return {
                "source": source, "entity": entity, "revision": revision,
                "status": "unknown",
            }

    def _event_verdict(self, ev: sqlite3.Row) -> dict:
        return {
            "status": "accepted",
            "event_id": ev["event_id"],
            "source": ev["source"],
            "entity": ev["entity"],
            "revision": ev["revision"],
            "op": ev["op"],
            "valid_time": ev["valid_time"],
            "value": None if ev["payload_json"] is None
            else json.loads(ev["payload_json"]),
            "target_event_id": ev["target_event_id"],
            "knowledge_seq": ev["knowledge_seq"],
            "batch_id": ev["batch_id"],
        }

    def _rejection_verdict(self, rj: sqlite3.Row) -> dict:
        out = {
            "status": "rejected",
            "event_id": rj["event_id"],
            "source": rj["source"],
            "entity": rj["entity"],
            "revision": rj["revision"],
            "reason_code": rj["reason_code"],
            "reason_detail": rj["reason_detail"],
            "batch_id": rj["batch_id"],
        }
        return out

    # ================================================================ ingest
    def ingest_batch(self, batch_id: str, records: list) -> dict:
        """Validate and atomically apply one change batch.

        Returns a per-record verdict summary.  Re-delivering an identical
        batch (same canonical bytes) returns the stored verdict and never
        allocates a second knowledge sequence.  One bad record never poisons
        the batch: valid records are still accepted.
        """
        if not _is_nonempty_str(batch_id):
            raise ValueError("batch_id must be a non-empty string")
        if not isinstance(records, list):
            raise ValueError("records must be a list")

        envelope = {"batch_id": batch_id, "records": records}
        try:
            raw_hash = hashlib.sha256(
                canonical_json(envelope).encode("utf-8")
            ).hexdigest()
        except (ValueError, TypeError) as exc:
            raise ValueError(f"batch is not serializable finite JSON: {exc}")

        with self._write_lock:
            prior = self._writer.execute(
                "SELECT raw_hash FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if prior is not None:
                if prior["raw_hash"] != raw_hash:
                    raise BatchConflictError(
                        f"batch_id {batch_id!r} already exists with different "
                        "contents; corrections require a new revision"
                    )
                return self._rebuild_result(batch_id)

            # ---- phase 1: per-record structural normalization -------------
            normalized: list[dict | None] = []
            structural: list[dict] = []
            for index, record in enumerate(records):
                norm, rejection = self._normalize_record(index, record)
                normalized.append(norm)
                if rejection is not None:
                    structural.append(rejection)

            ok_records = [n for n in normalized if n is not None]
            ev_counts, triple_counts = self._batch_duplicate_maps(ok_records)

            self._writer.execute("BEGIN IMMEDIATE")
            try:
                # Another process/connection may have won the batch first.
                raced = self._writer.execute(
                    "SELECT raw_hash FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if raced is not None:
                    if raced["raw_hash"] != raw_hash:
                        raise BatchConflictError(
                            f"batch_id {batch_id!r} already exists with "
                            "different contents"
                        )
                    self._writer.execute("ROLLBACK")
                    return self._rebuild_result(batch_id)

                # ---- phase 2: persist structural rejections ---------------
                for rej in structural:
                    self._insert_rejection(batch_id, rej)

                # ---- phase 3: semantic validation per record --------------
                candidates: dict[int, dict] = {}
                for index, norm in enumerate(normalized):
                    if norm is None:
                        continue
                    verdict = self._semantic_check(
                        norm, ev_counts, triple_counts
                    )
                    if verdict is not None:
                        self._insert_rejection(batch_id, verdict)
                    else:
                        candidates[index] = norm

                # ---- phase 4: retract target + cycle resolution -----------
                self._validate_retractions(candidates)
                for index, norm in list(candidates.items()):
                    if norm.get("_reject") is not None:
                        self._insert_rejection(batch_id, norm["_reject"])
                        del candidates[index]

                # ---- phase 5: knowledge sequence + interval re-splice -----
                accepted = [candidates[i] for i in sorted(candidates)]
                seq = None
                if accepted:
                    seq = int(self._writer.execute(
                        "SELECT COALESCE(MAX(knowledge_seq), 0) + 1 AS n "
                        "FROM events"
                    ).fetchone()["n"])
                    touched_entities: set[tuple[str, str]] = set()
                    # Upserts first so same-batch retract targets always exist.
                    for norm in accepted:
                        if norm["op"] == "upsert":
                            self._apply_upsert(norm, seq)
                            touched_entities.add(
                                (norm["source"], norm["entity"]))
                    for norm in accepted:
                        if norm["op"] == "retract":
                            self._apply_retract(norm, seq)
                            touched_entities.add(
                                (norm["source"], norm["entity"]))
                    for source, entity in touched_entities:
                        self._writer.execute(
                            "INSERT OR IGNORE INTO entity_versions"
                            "(source, entity, head_seq) VALUES(?, ?, ?)",
                            (source, entity, seq),
                        )
                    for index, norm in ((n["record_index"], n) for n in accepted):
                        self._insert_event(batch_id, norm, seq)

                self._writer.execute(
                    "INSERT INTO batches(batch_id, raw_hash, record_count, "
                    "accepted_count, rejected_count, knowledge_seq, ingested_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (batch_id, raw_hash, len(records), len(accepted),
                     len(records) - len(accepted), seq, _utc_now()),
                )

                if self._simulate_crash:
                    # Test hook: abort after every physical write but before
                    # COMMIT, emulating a process crash mid-transaction.
                    raise sqlite3.OperationalError(
                        "simulated crash before commit"
                    )
                self._writer.execute("COMMIT")
            except BaseException:
                self._writer.execute("ROLLBACK")
                raise

            return self._rebuild_result(batch_id)

    # ----------------------------------------------------- record validation
    def _normalize_record(self, index: int, record: Any):
        """Return (normalized_dict_or_None, rejection_dict_or_None)."""
        base = {"record_index": index}
        if not isinstance(record, dict):
            return None, {**base, "event_id": None, "source": None,
                          "entity": None, "revision": None, "op": None,
                          "valid_time": None,
                          "reason_code": "malformed_record",
                          "reason_detail": "record must be a JSON object"}
        unknown = sorted(set(record) - _ALLOWED_FIELDS)
        if unknown:
            return None, {**base, **self._ident_fields(record),
                          "reason_code": "unknown_field",
                          "reason_detail": f"undefined field(s): {', '.join(unknown)}"}

        def fail(code: str, detail: str):
            return None, {**base, **self._ident_fields(record),
                          "reason_code": code, "reason_detail": detail}

        event_id = record.get("event_id")
        if not _is_nonempty_str(event_id):
            return fail("invalid_event_id", REJECTION_REASONS["invalid_event_id"])
        source = record.get("source")
        if not _is_nonempty_str(source):
            return fail("invalid_source", REJECTION_REASONS["invalid_source"])
        entity = record.get("entity")
        if not _is_nonempty_str(entity):
            return fail("invalid_entity", REJECTION_REASONS["invalid_entity"])
        revision = record.get("revision")
        if not _is_int(revision) or revision < 0:
            return fail("invalid_revision", REJECTION_REASONS["invalid_revision"])
        op = record.get("op")
        if op not in _OPS:
            return fail("invalid_operation", REJECTION_REASONS["invalid_operation"])
        try:
            valid_time = parse_valid_time(record.get("valid_time"))
        except TimeParseError as exc:
            return fail("invalid_valid_time", str(exc))

        if op == "retract":
            target = record.get("target_event_id")
            if not _is_nonempty_str(target):
                return fail("retract_missing_target",
                            REJECTION_REASONS["retract_missing_target"])
            if record.get("value") is not None:
                return fail("retract_has_value",
                            REJECTION_REASONS["retract_has_value"])
            value = None
        else:
            if "value" not in record:
                return fail("malformed_record",
                            "upsert record requires a 'value' field")
            value = record["value"]
            try:
                canonical_json(value)
            except (ValueError, TypeError) as exc:
                return fail("malformed_record",
                            f"value is not finite serializable JSON: {exc}")
            target = None

        return {
            "record_index": index,
            "event_id": event_id,
            "source": source,
            "entity": entity,
            "revision": revision,
            "op": op,
            "valid_time": valid_time,
            "value": value,
            "target_event_id": target,
        }, None

    @staticmethod
    def _ident_fields(record: dict) -> dict:
        def clean(key: str):
            v = record.get(key)
            if key == "revision":
                return v if _is_int(v) else None
            return v if _is_nonempty_str(v) else None
        return {
            "event_id": clean("event_id"),
            "source": clean("source"),
            "entity": clean("entity"),
            "revision": clean("revision"),
            "op": record.get("op") if record.get("op") in _OPS else None,
            "valid_time": record.get("valid_time")
            if _is_nonempty_str(record.get("valid_time")) else None,
        }

    @staticmethod
    def _batch_duplicate_maps(records: list[dict]):
        ev_counts: dict[str, int] = {}
        triple_counts: dict[tuple, int] = {}
        for norm in records:
            ev_counts[norm["event_id"]] = ev_counts.get(norm["event_id"], 0) + 1
            key = (norm["source"], norm["entity"], norm["revision"])
            triple_counts[key] = triple_counts.get(key, 0) + 1
        return ev_counts, triple_counts

    def _semantic_check(self, norm: dict, ev_counts: dict,
                        triple_counts: dict) -> dict | None:
        base = {k: norm[k] for k in
                ("record_index", "event_id", "source", "entity",
                 "revision", "op", "valid_time")}

        def reject(code: str, detail: str) -> dict:
            return {**base, "reason_code": code, "reason_detail": detail}

        if ev_counts[norm["event_id"]] > 1:
            return reject("duplicate_event_id_in_batch",
                          REJECTION_REASONS["duplicate_event_id_in_batch"])
        # A re-used event_id is reported first: accepted history can never be
        # rewritten, regardless of which revision triple it lands on.
        prior_event = self._writer.execute(
            "SELECT 1 FROM events WHERE event_id=?", (norm["event_id"],)
        ).fetchone()
        prior_reject = self._writer.execute(
            "SELECT 1 FROM rejections WHERE event_id=? LIMIT 1",
            (norm["event_id"],),
        ).fetchone()
        if prior_event is not None or prior_reject is not None:
            return reject("event_id_already_recorded",
                          REJECTION_REASONS["event_id_already_recorded"])
        if triple_counts[(norm["source"], norm["entity"],
                          norm["revision"])] > 1:
            return reject("revision_conflict",
                          "duplicate source/entity/revision inside the batch")
        conflict = self._writer.execute(
            "SELECT event_id FROM events WHERE source=? AND entity=? AND revision=?",
            (norm["source"], norm["entity"], norm["revision"]),
        ).fetchone()
        if conflict is not None:
            return reject(
                "revision_conflict",
                f"revision already held by event {conflict['event_id']}",
            )
        return None

    def _validate_retractions(self, candidates: dict[int, dict]) -> None:
        retract_map = {
            norm["event_id"]: norm
            for norm in candidates.values()
            if norm["op"] == "retract"
        }
        # Several retracts may aim at one target; only the earliest survives.
        retract_targets: dict[str, list[dict]] = {}
        for norm in retract_map.values():
            retract_targets.setdefault(norm["target_event_id"], []).append(norm)
        # Same-batch upserts sharing a valid start supersede each other at
        # apply time (they run in record_index order); only the last owner
        # stays live, so retracting any earlier owner cannot take effect.
        start_owners: dict[tuple, list[dict]] = {}
        for norm in sorted(candidates.values(), key=lambda n: n["record_index"]):
            if norm["op"] == "upsert":
                key = (norm["source"], norm["entity"], norm["valid_time"])
                start_owners.setdefault(key, []).append(norm)
        superseded_pending = {
            norm["event_id"]
            for owners in start_owners.values()
            if len(owners) > 1
            for norm in owners[:-1]
        }

        for norm in candidates.values():
            if norm["op"] != "retract":
                continue
            target_id = norm["target_event_id"]

            # Cycle walk over retractions surviving in this batch only.
            seen: set[str] = set()
            cursor = norm["event_id"]
            while cursor in retract_map:
                if cursor in seen:
                    norm["_reject"] = self._make_rejection(
                        norm, "retract_cycle",
                        f"retract chain loops back through event {cursor}")
                    break
                seen.add(cursor)
                cursor = retract_map[cursor]["target_event_id"]
            if "_reject" in norm:
                continue

            state, target = self._resolve_target(norm, target_id, candidates)
            if state == "unknown":
                norm["_reject"] = self._make_rejection(
                    norm, "unknown_retract_target",
                    f"no accepted upsert event {target_id!r}")
            elif state == "other_entity":
                norm["_reject"] = self._make_rejection(
                    norm, "unknown_retract_target",
                    f"target {target_id!r} belongs to "
                    f"{target['source']}/{target['entity']}, not "
                    f"{norm['source']}/{norm['entity']}")
            elif state == "inactive":
                norm["_reject"] = self._make_rejection(
                    norm, "retract_target_inactive",
                    f"target fact {target_id!r} is already retracted or superseded")
            elif state == "pending_upsert" and target_id in superseded_pending:
                norm["_reject"] = self._make_rejection(
                    norm, "retract_target_inactive",
                    f"target fact {target_id!r} is superseded by another "
                    "upsert at the same valid start in this batch")
            elif len(retract_targets[target_id]) > 1:
                first = min(retract_targets[target_id],
                            key=lambda n: n["record_index"])
                if norm is not first:
                    norm["_reject"] = self._make_rejection(
                        norm, "retract_target_inactive",
                        f"target fact {target_id!r} is already being "
                        f"retracted by event {first['event_id']} in this batch")

    def _resolve_target(self, norm: dict, target_id: str,
                        candidates: dict[int, dict]):
        """Resolve a retract target against history and the open transaction."""
        event = self._writer.execute(
            "SELECT * FROM events WHERE event_id=?", (target_id,)
        ).fetchone()
        if event is not None:
            if event["op"] != "upsert":
                return "unknown", None
            if (event["source"], event["entity"]) != (
                    norm["source"], norm["entity"]):
                return "other_entity", event
            live = self._writer.execute(
                "SELECT 1 FROM facts WHERE event_id=? "
                "AND close_seq IS NULL LIMIT 1",
                (target_id,),
            ).fetchone()
            if live is None:
                return "inactive", event
            return "live", event
        # Target may be an upsert accepted earlier within this same batch.
        for cand in candidates.values():
            if cand["event_id"] == target_id and cand["op"] == "upsert":
                if (cand["source"], cand["entity"]) != (
                        norm["source"], norm["entity"]):
                    return "other_entity", cand
                return "pending_upsert", cand
        return "unknown", None

    @staticmethod
    def _make_rejection(norm: dict, code: str, detail: str) -> dict:
        return {
            "record_index": norm["record_index"],
            "event_id": norm["event_id"],
            "source": norm["source"],
            "entity": norm["entity"],
            "revision": norm["revision"],
            "op": norm["op"],
            "valid_time": norm["valid_time"],
            "reason_code": code,
            "reason_detail": detail,
        }

    # --------------------------------------------------------- fact splicing
    #
    # Invariant: an existing facts row is only ever closed (close_seq set once)
    # or left untouched; rows are never updated or deleted.  Rows visible at an
    # earlier cutoff are therefore immutable, which is what makes historical
    # queries bit-stable.  Facts carry no explicit valid_to: the point-in-time
    # query selects the greatest valid_from <= as_of, so later starts take over
    # at their own inclusive boundary and earlier facts stay available inside a
    # gap left by a retracted later fact.
    def _apply_upsert(self, norm: dict, seq: int) -> None:
        s, e, vf = norm["source"], norm["entity"], norm["valid_time"]
        # A higher revision at the same valid start closes every still-open
        # knowledge version at that start; other starts are unaffected.
        self._writer.execute(
            "UPDATE facts SET close_seq=? "
            "WHERE source=? AND entity=? AND valid_from=? AND close_seq IS NULL",
            (seq, s, e, vf),
        )
        self._writer.execute(
            "INSERT INTO facts(source, entity, valid_from, create_seq, "
            "close_seq, value_json, event_id) "
            "VALUES(?, ?, ?, ?, NULL, ?, ?)",
            (s, e, vf, seq, canonical_json(norm["value"]),
             norm["event_id"]),
        )

    def _apply_retract(self, norm: dict, seq: int) -> None:
        s, e = norm["source"], norm["entity"]
        target_id = norm["target_event_id"]
        # A retraction only closes the target fact's knowledge versions.  No
        # blocking interval is inserted, so an earlier fact still fills the
        # target's valid-time span at the new cutoff.
        self._writer.execute(
            "UPDATE facts SET close_seq=? "
            "WHERE source=? AND entity=? AND event_id=? AND close_seq IS NULL",
            (seq, s, e, target_id),
        )

    def _insert_event(self, batch_id: str, norm: dict, seq: int) -> None:
        self._writer.execute(
            "INSERT INTO events(record_index, event_id, source, entity, "
            "revision, op, valid_time, payload_json, target_event_id, "
            "knowledge_seq, batch_id, ingested_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (norm["record_index"], norm["event_id"], norm["source"],
             norm["entity"], norm["revision"], norm["op"], norm["valid_time"],
             canonical_json(norm["value"]) if norm["op"] == "upsert" else None,
             norm["target_event_id"], seq, batch_id, _utc_now()),
        )

    def _insert_rejection(self, batch_id: str, rej: dict) -> None:
        self._writer.execute(
            "INSERT INTO rejections(batch_id, record_index, event_id, source, "
            "entity, revision, op, valid_time, reason_code, reason_detail) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (batch_id, rej["record_index"], rej.get("event_id"),
             rej.get("source"), rej.get("entity"), rej.get("revision"),
             rej.get("op"), rej.get("valid_time"),
             rej["reason_code"], rej["reason_detail"]),
        )

    # ------------------------------------------------------------- rebuilders
    def _rebuild_result(self, batch_id: str, batch_row: sqlite3.Row | None = None
                        ) -> dict:
        if batch_row is None:
            batch_row = self._writer.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
        accepted_rows = self._writer.execute(
            "SELECT event_id FROM events WHERE batch_id=? ORDER BY record_index",
            (batch_id,),
        ).fetchall()
        rejected_rows = self._writer.execute(
            "SELECT record_index, event_id, source, entity, revision, op, "
            "valid_time, reason_code, reason_detail FROM rejections "
            "WHERE batch_id=? ORDER BY record_index, id",
            (batch_id,),
        ).fetchall()
        return {
            "batch_id": batch_id,
            "knowledge_seq": batch_row["knowledge_seq"],
            "record_count": batch_row["record_count"],
            "accepted_count": batch_row["accepted_count"],
            "rejected_count": batch_row["rejected_count"],
            "accepted": [r["event_id"] for r in accepted_rows],
            "rejected": [dict(r) for r in rejected_rows],
        }

    # ================================================================== query
    def query_materialized(self, sources: Iterable[str], entities: Iterable[str],
                           as_of: str, known_through: int | None = None) -> dict:
        """Return materialized values per source/entity pair.

        Every pair gets an explicit entry; an entity with no valid value at
        the cutoff returns ``state='empty'`` with null value/event fields.
        The whole query runs on one WAL snapshot so concurrent commits cannot
        mix intervals from before and after a batch.
        """
        source_list = self._clean_name_set(sources, "sources")
        entity_list = self._clean_name_set(entities, "entities")
        as_of_norm = parse_valid_time(as_of)
        if known_through is None:
            cutoff = self.head_sequence()
        else:
            if not _is_int(known_through) or known_through < 0:
                raise ValueError("known_through must be a non-negative integer")
            cutoff = known_through

        pairs = sorted((s, e) for s in source_list for e in entity_list)
        reader = self._open_reader()
        try:
            reader.execute("BEGIN")
            results = []
            for source, entity in pairs:
                results.append(self._query_one(
                    reader, source, entity, as_of_norm, cutoff))
            reader.execute("COMMIT")
        except BaseException:
            reader.execute("ROLLBACK")
            reader.close()
            raise
        reader.close()
        return {
            "as_of": as_of_norm,
            "known_through": cutoff,
            "results": results,
        }

    @staticmethod
    def _clean_name_set(values: Iterable[str], label: str) -> list[str]:
        items = list(values)
        if not items:
            raise ValueError(f"{label} must not be empty")
        if not all(_is_nonempty_str(v) for v in items):
            raise ValueError(f"{label} must contain only non-empty strings")
        if len(set(items)) != len(items):
            raise ValueError(f"{label} must not contain duplicates")
        return items

    def _open_reader(self) -> sqlite3.Connection:
        from urllib.request import pathname2url
        uri = "file:" + pathname2url(self.path) + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def _query_one(self, conn: sqlite3.Connection, source: str, entity: str,
                   as_of: str, cutoff: int) -> dict:
        empty = {
            "source": source, "entity": entity, "state": "empty",
            "value": None, "event_id": None, "event_knowledge_seq": None,
            "retracted_by_event_id": None,
        }
        if cutoff <= 0:
            return empty
        head = conn.execute(
            "SELECT MAX(head_seq) AS head_seq FROM entity_versions "
            "WHERE source=? AND entity=? AND head_seq <= ?",
            (source, entity, cutoff),
        ).fetchone()
        if head is None:
            return empty
        # Pick the greatest valid start not later than as_of among facts whose
        # knowledge interval contains the cutoff.  A tie on valid_from resolves
        # to the highest create_seq (higher revision at the same start).
        fact = conn.execute(
            "SELECT * FROM facts WHERE source=? AND entity=? "
            "AND create_seq <= ? AND (close_seq IS NULL OR close_seq > ?) "
            "AND valid_from <= ? "
            "ORDER BY valid_from DESC, create_seq DESC LIMIT 1",
            (source, entity, cutoff, cutoff, as_of),
        ).fetchone()
        if fact is None:
            # No value at this cutoff/as_of.  A retraction explains the gap
            # only when the target was effective at as_of and the target's
            # knowledge interval was actually closed by that retraction at or
            # before the cutoff.  Retractions superseded by later revisions are
            # not reported.  The value stays explicitly empty either way.
            marker = conn.execute(
                "SELECT r.event_id AS event_id FROM events r "
                "JOIN events t ON t.event_id = r.target_event_id "
                "WHERE r.op='retract' AND r.source=? AND r.entity=? "
                "AND r.knowledge_seq <= ? AND t.valid_time <= ? "
                "AND EXISTS (SELECT 1 FROM facts f WHERE f.event_id=t.event_id "
                "  AND f.close_seq = r.knowledge_seq) "
                "ORDER BY r.knowledge_seq DESC LIMIT 1",
                (source, entity, cutoff, as_of),
            ).fetchone()
            return {**empty,
                    "retracted_by_event_id": marker["event_id"] if marker
                    else None}
        return {
            "source": source,
            "entity": entity,
            "state": "present",
            "value": json.loads(fact["value_json"]),
            "event_id": fact["event_id"],
            "event_knowledge_seq": fact["create_seq"],
            "retracted_by_event_id": None,
        }
