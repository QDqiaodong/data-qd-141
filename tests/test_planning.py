import itertools
import json
import subprocess
import sys
import tempfile
import unittest
from functools import lru_cache
from pathlib import Path

from sequence_align.alignment import align
from sequence_align.planning import (
    MAX_CHANGES_PER_STAGE,
    MAX_STAGES,
    candidates,
    plan,
)

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Helpers


def fixture():
    return json.loads((ROOT / "fixtures/input.json").read_text())


def replay_alignment(before, after):
    result = align(list(before), list(after))
    i = j = 0
    rebuilt = []
    for op in result["operations"]:
        assert (op["before_index"], op["after_index"]) == (i, j)
        if op["source"] is not None:
            assert op["source"] == before[i]
            i += 1
        if op["target"] is not None:
            rebuilt.append(op["target"])
            j += 1
    assert i == len(before) and rebuilt == list(after)
    assert result["distance"] == sum(op["op"] != "match" for op in result["operations"])
    return result


def candidate_map(before, after):
    return {(entry["kind"], entry["field"]): entry for entry in candidates(before, after)}


def check_plan(before, after, consumers, report):
    """Independently replay a reported plan and assert every contract."""
    assert report["status"] == "satisfiable"
    assert report["reason"] is None
    assert report["after"] == list(after)
    stages = report["plan"]
    assert 1 <= len(stages) <= MAX_STAGES
    assert [stage["stage"] for stage in stages] == list(range(1, len(stages) + 1))

    numbers = candidate_map(before, after)
    add_numbers = {
        entry["number"]: entry["field"]
        for entry in numbers.values()
        if entry["kind"] == "add"
    }
    drop_numbers = {
        entry["number"]: entry["field"]
        for entry in numbers.values()
        if entry["kind"] == "drop"
    }
    expected_adds = {entry["field"] for entry in numbers.values() if entry["kind"] == "add"}
    expected_drops = {entry["field"] for entry in numbers.values() if entry["kind"] == "drop"}

    present = set(before)
    switched = set()
    executed_adds, executed_drops = [], []
    for stage in stages:
        changes = len(stage["add"]) + len(stage["drop"])
        assert changes <= MAX_CHANGES_PER_STAGE
        assert stage["add"] or stage["drop"] or stage["switch"], "idle stage"
        for entry in stage["add"]:
            field = entry["field"]
            assert add_numbers[entry["number"]] == field
            assert field not in present, "add target already present"
            assert field in after and field not in before
            present.add(field)
            executed_adds.append(field)
        for field in stage["switch"]:
            assert field in {consumer["id"] for consumer in consumers}
            assert field not in switched, "consumer switched twice"
            consumer = next(c for c in consumers if c["id"] == field)
            lo, hi = consumer["window"]
            assert lo <= stage["stage"] <= hi, "switch outside window"
            assert set(consumer["reads_before"]) <= present
            assert set(consumer["reads_after"]) <= present
            switched.add(field)
        for entry in stage["drop"]:
            field = entry["field"]
            assert drop_numbers[entry["number"]] == field
            assert field in present, "drop target absent"
            present.discard(field)
            executed_drops.append(field)
        for consumer in consumers:
            if consumer["id"] in switched:
                assert set(consumer["reads_after"]) <= present
            else:
                assert set(consumer["reads_before"]) <= present
        assert set(stage["fields"]) == present

    assert switched == {consumer["id"] for consumer in consumers}
    assert sorted(executed_adds) == sorted(expected_adds)
    assert sorted(executed_drops) == sorted(expected_drops)
    assert present == set(after)
    return present


def report_key(report):
    return len(report["plan"]), [
        (
            tuple(stage["switch"]),
            tuple(sorted([entry["number"] for entry in stage["add"]]
                         + [entry["number"] for entry in stage["drop"]])),
        )
        for stage in report["plan"]
    ]


# ---------------------------------------------------------------------------
# Independent exhaustive reference
#
# Item-level depth-first search over stages, enumerating every legal add/drop
# pair and every switch subset; no grouping, no dominance pruning.  Small
# inputs only, used to prove unsatisfiability and optimality.


def reference(before, after, consumers, limit=MAX_STAGES):
    before = tuple(before)
    after = tuple(after)
    adds = tuple(field for field in after if field not in before)
    drops = tuple(field for field in before if field not in after)
    ids = [consumer["id"] for consumer in consumers]
    windows = [tuple(consumer["window"]) for consumer in consumers]
    reads_before = [frozenset(consumer["reads_before"]) for consumer in consumers]
    reads_after = [frozenset(consumer["reads_after"]) for consumer in consumers]
    full_adds, full_drops = frozenset(adds), frozenset(drops)
    count = len(consumers)
    numbers = {
        (entry["kind"], entry["field"]): entry["number"]
        for entry in candidates(before, after)
    }

    def actions(stage, added, dropped, switched):
        present = (frozenset(before) - dropped) | added
        if stage > limit:
            return
        add_pool = [field for field in adds if field not in added]
        drop_pool = [field for field in drops if field not in dropped]
        add_choices = [()] + [(field,) for field in add_pool]
        add_choices += list(itertools.combinations(add_pool, 2))
        for add_choice in add_choices:
            budget = 2 - len(add_choice)
            after_choice = present | frozenset(add_choice)
            drop_choices = [()] + [(field,) for field in drop_pool]
            if budget >= 2:
                drop_choices += list(itertools.combinations(drop_pool, 2))
            for drop_choice in drop_choices:
                if len(drop_choice) > budget:
                    continue
                remain = after_choice - frozenset(drop_choice)
                eligible = [
                    c
                    for c in range(count)
                    if c not in switched
                    and windows[c][0] <= stage <= windows[c][1]
                    and reads_after[c] <= after_choice
                    and reads_before[c] <= after_choice
                ]
                must = [
                    c
                    for c in range(count)
                    if c not in switched and windows[c][1] <= stage
                ]
                if any(c not in eligible for c in must):
                    continue
                optional = [c for c in eligible if c not in must]
                for size in range(len(optional) + 1):
                    for extra in itertools.combinations(optional, size):
                        selected = tuple(must) + extra
                        if not add_choice and not drop_choice and not selected:
                            continue
                        now = switched | frozenset(selected)
                        if any(
                            reads_before[c] & frozenset(drop_choice)
                            for c in range(count)
                            if c not in now
                        ):
                            continue
                        if any(
                            c not in now
                            and (
                                windows[c][1] <= stage
                                or not reads_before[c] <= remain
                            )
                            for c in range(count)
                        ):
                            continue
                        key = (
                            tuple(sorted(ids[c] for c in selected)),
                            tuple(sorted(
                                [numbers[("add", field)] for field in add_choice]
                                + [numbers[("drop", field)] for field in drop_choice]
                            )),
                        )
                        yield key, add_choice, drop_choice, selected, now

    @lru_cache(maxsize=None)
    def best(stage, added, dropped, switched):
        if added == full_adds and dropped == full_drops and len(switched) == count:
            return 0
        if stage > limit:
            return None
        result = None
        for _, add_choice, drop_choice, _, now in actions(stage, added, dropped, switched):
            tail = best(
                stage + 1,
                added | frozenset(add_choice),
                dropped | frozenset(drop_choice),
                now,
            )
            if tail is not None and (result is None or tail + 1 < result):
                result = tail + 1
        return result

    total = best(1, frozenset(), frozenset(), frozenset())
    if total is None:
        return None
    stage = 1
    added, dropped, switched = frozenset(), frozenset(), frozenset()
    remaining = total
    sequence = []
    while remaining:
        options = []
        for key, add_choice, drop_choice, _, now in actions(
            stage, added, dropped, switched
        ):
            tail = best(
                stage + 1,
                added | frozenset(add_choice),
                dropped | frozenset(drop_choice),
                now,
            )
            if tail is not None and tail + 1 == remaining:
                options.append((key, add_choice, drop_choice, now))
        options.sort(key=lambda entry: entry[0])
        key, add_choice, drop_choice, now = options[0]
        sequence.append(key)
        added |= frozenset(add_choice)
        dropped |= frozenset(drop_choice)
        switched = now
        stage += 1
        remaining -= 1
    return total, sequence


# ---------------------------------------------------------------------------
# Candidate generation


class Candidates(unittest.TestCase):
    def test_distance_and_replay(self):
        before = ["extract", "clean", "load"]
        after = ["extract", "validate", "load", "audit"]
        result = replay_alignment(before, after)
        self.assertEqual(result["distance"], 2)
        self.assertEqual(result["counts"], {
            "match": 2, "replace": 1, "delete": 0, "insert": 1,
        })

    def test_sets_are_strict_diffs(self):
        before = ["a", "b", "c"]
        after = ["c", "b", "d", "e"]
        entries = candidates(before, after)
        self.assertEqual(
            {(entry["kind"], entry["field"]) for entry in entries},
            {("drop", "a"), ("add", "d"), ("add", "e")},
        )
        self.assertEqual([entry["number"] for entry in entries], [1, 2, 3])

    def test_shared_reorder_makes_no_candidate(self):
        self.assertEqual(candidates(["a", "b"], ["b", "a"]), [])
        self.assertEqual(candidates(["x", "y"], ["y", "x"]), [])

    def test_numbering_follows_first_alignment_operation(self):
        before = ["a", "b", "c", "d"]
        after = ["a", "x", "d", "y"]
        operations = align(before, after)["operations"]
        entries = candidates(before, after)
        # Independent recomputation of the documented ordering rule.
        add_set = {f for f in after if f not in before}
        drop_set = {f for f in before if f not in after}
        first = {}
        for index, op in enumerate(operations):
            for field in (op["source"], op["target"]):
                if field in add_set or field in drop_set:
                    first.setdefault(field, index)
        ordered = sorted(
            [(first[f], 0, f) for f in add_set] + [(first[f], 1, f) for f in drop_set]
        )
        self.assertEqual(
            [(entry["kind"], entry["field"]) for entry in entries],
            [("add" if rank == 0 else "drop", field) for _, rank, field in ordered],
        )
        self.assertEqual(
            [entry["number"] for entry in entries], list(range(1, len(entries) + 1))
        )

    def test_replace_grouping_keeps_candidate_identities(self):
        before = ["clean"]
        after = ["validate"]
        entries = candidates(before, after)
        by_kind = {(entry["kind"], entry["field"]): entry for entry in entries}
        add = by_kind[("add", "validate")]
        drop = by_kind[("drop", "clean")]
        self.assertEqual((add["number"], drop["number"]), (1, 2))
        self.assertEqual(add["replace_with"], "clean")
        self.assertIsNone(drop["replace_with"])
        # A replace is a display grouping, not a third candidate identity.
        self.assertEqual({entry["kind"] for entry in entries}, {"add", "drop"})

    def test_stable_numbering_is_deterministic(self):
        before = ["a", "b", "c"]
        after = ["c", "d", "e"]
        first = candidates(before, after)
        self.assertEqual(first, candidates(before, after))
        self.assertEqual([entry["number"] for entry in first],
                         list(range(1, len(first) + 1)))


# ---------------------------------------------------------------------------
# Input contract


class Validation(unittest.TestCase):
    def bad(self, data):
        with self.assertRaises((ValueError, TypeError)):
            plan(data)

    def base(self):
        return {
            "before": ["a", "b"],
            "after": ["a", "c"],
            "consumers": [
                {"id": "c0", "reads_before": ["b"], "reads_after": ["c"],
                 "window": [1, 8]},
            ],
        }

    def test_valid(self):
        self.assertEqual(plan(self.base())["status"], "satisfiable")

    def test_unknown_and_missing_fields(self):
        data = self.base()
        data["extra"] = 1
        self.bad(data)
        del data["extra"]
        del data["consumers"]
        self.bad(data)
        consumer = self.base()
        consumer["consumers"][0]["extra"] = 1
        self.bad(consumer)
        missing = self.base()
        del missing["consumers"][0]["window"]
        self.bad(missing)

    def test_field_arrays(self):
        data = self.base()
        data["before"] = []
        self.bad(data)
        data = self.base()
        data["after"] = []
        self.bad(data)
        data = self.base()
        data["before"] = ["a", "a"]
        self.bad(data)
        data = self.base()
        data["after"] = ["a", "c", "c"]
        self.bad(data)
        data = self.base()
        data["before"] = ["a", "b", "d", "e", "f", "g", "h", "i", "j"]
        self.bad(data)
        data = self.base()
        data["before"] = ["a", " b"]
        self.bad(data)
        data = self.base()
        data["before"] = ["a", ""]
        self.bad(data)

    def test_consumer_contract(self):
        data = self.base()
        data["consumers"] = []
        self.bad(data)
        data = self.base()
        data["consumers"] = data["consumers"] * 7
        self.bad(data)
        data = self.base()
        data["consumers"].append(dict(data["consumers"][0]))
        self.bad(data)
        data = self.base()
        data["consumers"][0]["id"] = ""
        self.bad(data)
        data = self.base()
        data["consumers"][0]["reads_before"] = ["z"]
        self.bad(data)
        data = self.base()
        data["consumers"][0]["reads_after"] = ["z"]
        self.bad(data)
        data = self.base()
        data["consumers"][0]["reads_before"] = ["b", "b"]
        self.bad(data)

    def test_windows(self):
        invalid = [
            [0, 1], [2, 1], [1, 9], [1], [1, 2, 3], [1.0, 2], [True, 2],
            ["1", 2], None,
        ]
        for window in invalid:
            data = self.base()
            data["consumers"][0]["window"] = window
            self.bad(data)
        for window in ([1, 1], [1, 8]):
            data = {"before": ["a"], "after": ["a"],
                    "consumers": [{"id": "c0", "reads_before": [],
                                   "reads_after": [], "window": window}]}
            self.assertEqual(plan(data)["status"], "satisfiable")
        # A window that only opens in stage 8 cannot avoid idle stages.
        data = {"before": ["a"], "after": ["a"],
                "consumers": [{"id": "c0", "reads_before": [],
                               "reads_after": [], "window": [8, 8]}]}
        self.assertEqual(plan(data)["status"], "unsatisfiable")

    def test_duplicate_json_keys_rejected_by_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "dup.json"
            source.write_text('{"before": ["a"], "before": ["b"]}')
            from sequence_align.common import read

            with self.assertRaises(ValueError):
                read(source)


# ---------------------------------------------------------------------------
# Plan replay, windows and capacity


class PlanProperties(unittest.TestCase):
    def test_sample_replacement(self):
        data = fixture()
        data["consumers"] = [
            {"id": "reader", "reads_before": ["clean"],
             "reads_after": ["validate"], "window": [1, 8]},
        ]
        report = plan(data)
        check_plan(data["before"], data["after"], data["consumers"], report)
        self.assertEqual(len(report["plan"]), 2)
        self.assertTrue(any(stage["add"] for stage in report["plan"]))
        self.assertEqual(report["plan"][-1]["fields"], data["after"])

    def test_minimal_single_stage(self):
        data = fixture()
        data["consumers"] = [
            {"id": "reader", "reads_before": ["clean"],
             "reads_after": ["validate"], "window": [1, 8]},
        ]
        report = plan(data)
        self.assertEqual(len(report["plan"]), 2)  # two adds + one drop exceed capacity 2

    def test_capacity_two_changes_per_stage(self):
        before = ["a", "b", "c", "d"]
        after = ["w", "x", "y", "z"]
        consumers = [
            {"id": "c0", "reads_before": [], "reads_after": [], "window": [1, 8]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        self.assertEqual(report["status"], "satisfiable")
        self.assertEqual(len(report["plan"]), max(4, 4))
        for stage in report["plan"]:
            self.assertLessEqual(len(stage["add"]) + len(stage["drop"]), 2)
        check_plan(before, after, consumers, report)

    def test_no_idle_stage(self):
        # The only switch happens in stage 3, so stages 1 and 2 would be idle.
        before = ["a"]
        after = ["a"]
        consumers = [
            {"id": "c0", "reads_before": ["a"], "reads_after": ["a"], "window": [3, 3]},
        ]
        self.assertIsNone(reference(before, after, consumers))
        report = plan({"before": before, "after": after, "consumers": consumers})
        self.assertEqual(report["status"], "unsatisfiable")
        self.assertIsNone(report["plan"])
        self.assertIn("blocked_candidates", report["reason"])
        self.assertIn("blocked_consumers", report["reason"])

    def test_switch_once_and_windows(self):
        before = ["a", "b"]
        after = ["c"]
        consumers = [
            {"id": "c0", "reads_before": ["a"], "reads_after": ["c"], "window": [1, 2]},
            {"id": "c1", "reads_before": ["b"], "reads_after": [], "window": [2, 3]},
            {"id": "c2", "reads_before": [], "reads_after": ["c"], "window": [3, 3]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        check_plan(before, after, consumers, report)
        self.assertEqual(reference(before, after, consumers), report_key(report))

    def test_add_before_reference_and_drop_after_switch(self):
        before = ["old"]
        after = ["new"]
        consumers = [
            {"id": "c0", "reads_before": ["old"], "reads_after": ["new"],
             "window": [1, 8]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        check_plan(before, after, consumers, report)
        stages = report["plan"]
        add_stage = next(stage["stage"] for stage in stages if stage["add"])
        switch_stage = next(stage["stage"] for stage in stages if stage["switch"])
        drop_stage = next(stage["stage"] for stage in stages if stage["drop"])
        self.assertLessEqual(add_stage, switch_stage)
        self.assertGreaterEqual(drop_stage, switch_stage)

    def test_replacement_order_add_switch_drop(self):
        before = ["a", "clean", "b"]
        after = ["a", "validate", "b"]
        consumers = [
            {"id": "c0", "reads_before": ["clean"], "reads_after": ["validate"],
             "window": [1, 8]},
            {"id": "c1", "reads_before": ["clean"], "reads_after": ["validate"],
             "window": [3, 8]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        check_plan(before, after, consumers, report)
        self.assertEqual(
            [(entry["kind"], entry["field"]) for entry in candidates(before, after)],
            [("add", "validate"), ("drop", "clean")],
        )
        switch_stage = max(
            stage["stage"] for stage in report["plan"] if stage["switch"]
        )
        drop_stage = next(
            stage["stage"] for stage in report["plan"] if stage["drop"]
        )
        self.assertGreaterEqual(drop_stage, switch_stage)
        self.assertEqual(report["plan"][-1]["fields"], after)


# ---------------------------------------------------------------------------
# Oracle comparisons: unsatisfiability and optimality


class OracleComparison(unittest.TestCase):
    def compare(self, data):
        before, after, consumers = data["before"], data["after"], data["consumers"]
        expected = reference(before, after, consumers)
        report = plan(data)
        if expected is None:
            self.assertEqual(report["status"], "unsatisfiable", data)
            self.assertIsNone(report["plan"], data)
            self.assertEqual(report["after"], list(after))
            return
        check_plan(before, after, consumers, report)
        self.assertEqual(report_key(report), expected, data)

    def test_exhaustive_small(self):
        words = [
            subset
            for size in (1, 2, 3)
            for subset in itertools.combinations("abc", size)
        ]
        checked = 0
        for before in words:
            for after in words:
                for windows in itertools.product([(1, 1), (1, 2), (2, 2)], repeat=2):
                    consumers = [
                        {
                            "id": f"c{i}",
                            "reads_before": [f for f in before if f in ("a",) or i == 1],
                            "reads_after": [f for f in after if f in ("b",) or i == 0],
                            "window": list(windows[i]),
                        }
                        for i in range(2)
                    ]
                    self.compare({"before": list(before), "after": list(after),
                                  "consumers": consumers})
                    checked += 1
        self.assertGreater(checked, 400)

    def test_greedy_dead_end_is_not_unsatisfiable(self):
        # Greedily switching the first eligible consumer and dropping at once
        # strands c1: its window closes before it can read the added field.
        before = ["old"]
        after = ["new"]
        consumers = [
            {"id": "c0", "reads_before": [], "reads_after": ["new"],
             "window": [1, 1]},
            {"id": "c1", "reads_before": ["old"], "reads_after": [],
             "window": [1, 2]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        self.assertEqual(report["status"], "satisfiable")
        check_plan(before, after, consumers, report)
        self.assertEqual(reference(before, after, consumers), report_key(report))

    def test_unsatisfiable_has_exhaustive_proof(self):
        before = ["a", "b", "c"]
        after = ["a", "x", "y", "z"]
        consumers = [
            {"id": "c0", "reads_before": ["b", "c"], "reads_after": ["x", "y", "z"],
             "window": [1, 1]},
        ]
        # Three adds must precede the only possible switch (stage 1), but a
        # stage admits two changes, so no plan exists.
        self.assertIsNone(reference(before, after, consumers))
        report = plan({"before": before, "after": after, "consumers": consumers})
        self.assertEqual(report["status"], "unsatisfiable")
        self.assertIsNone(report["plan"])
        blocked = report["reason"]["blocked_candidates"]
        self.assertEqual(
            [(entry["kind"], entry["field"]) for entry in blocked],
            [(entry["kind"], entry["field"]) for entry in candidates(before, after)],
        )
        for entry in blocked:
            self.assertIn("capacity_per_stage", entry["conditions"])
            self.assertIn("precedence", entry["conditions"])
        consumer = report["reason"]["blocked_consumers"][0]
        self.assertEqual(consumer["id"], "c0")
        self.assertEqual(consumer["conditions"]["exactly_one_switch_within_window"], [1, 1])

    def test_stage_limit_is_eight(self):
        before = ["b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8"]
        after = ["a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"]
        consumers = [{"id": "c0", "reads_before": [], "reads_after": [],
                      "window": [1, 8]}]
        data = {"before": before, "after": after, "consumers": consumers}
        # More than 8 fields is rejected outright.
        with self.assertRaises(ValueError):
            plan(data)
        # 8 adds plus 8 drops need 8 stages of two changes: feasible.
        report = plan({
            "before": before[:8], "after": after[:8], "consumers": consumers,
        })
        self.assertEqual(len(report["plan"]), 8)
        # 8 adds and 7 drops still fit 8 stages; 8 and 8 with a tight window do not.
        tight = [{"id": "c0", "reads_before": before[:8],
                  "reads_after": after[:8], "window": [8, 8]}]
        self.assertEqual(
            plan({"before": before[:8], "after": after[:8],
                  "consumers": tight})["status"],
            "unsatisfiable",
        )


# ---------------------------------------------------------------------------
# Linked acceptance: window narrowing, capacity conflicts, widening


class LinkedAcceptance(unittest.TestCase):
    before = ["a", "old", "b"]
    after = ["a", "new", "b"]

    def wide(self):
        return [
            {"id": "fast", "reads_before": ["old"], "reads_after": ["new"],
             "window": [1, 8]},
            {"id": "slow", "reads_before": ["old"], "reads_after": ["new"],
             "window": [1, 8]},
        ]

    def test_narrowing_window_delays_drop(self):
        before = ["a", "old", "b"]
        after = ["a", "new1", "new2", "b"]
        wide = [
            {"id": "fast", "reads_before": ["old"], "reads_after": ["new1"],
             "window": [1, 8]},
            {"id": "slow", "reads_before": ["old"],
             "reads_after": ["new1", "new2"], "window": [1, 8]},
        ]
        baseline = plan({"before": before, "after": after, "consumers": wide})
        check_plan(before, after, wide, baseline)
        baseline_drop = next(
            stage["stage"] for stage in baseline["plan"] if stage["drop"]
        )

        consumers = [dict(wide[0]), dict(wide[1])]
        consumers[1]["window"] = [3, 8]
        narrow = plan({"before": before, "after": after, "consumers": consumers})
        check_plan(before, after, consumers, narrow)
        narrow_drop = next(
            stage["stage"] for stage in narrow["plan"] if stage["drop"]
        )
        self.assertGreater(narrow_drop, baseline_drop)
        for stage in narrow["plan"]:
            if stage["stage"] < narrow_drop:
                self.assertIn("old", stage["fields"])
        self.assertEqual(reference(before, after, consumers), report_key(narrow))

    def test_narrowing_causes_capacity_conflict(self):
        before = ["old", "s1", "s2", "s3"]
        after = ["new", "s1", "s2", "s3", "n2", "n3"]
        def consumers(window):
            return [
                {"id": "fast", "reads_before": ["old"], "reads_after": ["new"],
                 "window": [1, 1]},
                {"id": "slow", "reads_before": ["old"], "reads_after": [],
                 "window": list(window)},
            ]

        # With the wide window the four changes (one add for ``fast``, two adds,
        # one drop) spread over stages 1..4.
        wide = consumers([4, 8])
        baseline = plan({"before": before, "after": after, "consumers": wide})
        self.assertEqual(baseline["status"], "satisfiable")
        check_plan(before, after, wide, baseline)

        # Postponing ``slow`` to stage 5 leaves stage 5 with the switch alone
        # while stages 2..4 hold only three non-switch actions: the schedule
        # would need an idle stage, which is forbidden, so there is no plan.
        narrow = consumers([5, 8])
        self.assertIsNone(reference(before, after, narrow))
        report = plan({"before": before, "after": after, "consumers": narrow})
        self.assertEqual(report["status"], "unsatisfiable")
        self.assertIsNone(report["plan"])
        reason = report["reason"]
        self.assertEqual(reason["stage_limit"], 8)
        self.assertEqual(reason["capacity_per_stage"], 2)
        drop = next(entry for entry in reason["blocked_candidates"]
                    if entry["kind"] == "drop")
        self.assertEqual(drop["conditions"]["dependent_consumers"],
                         ["fast", "slow"])
        self.assertIn("must run after", drop["conditions"]["precedence"])
        consumer = next(entry for entry in reason["blocked_consumers"]
                        if entry["id"] == "slow")
        self.assertEqual(consumer["conditions"]["exactly_one_switch_within_window"],
                         [5, 8])
        self.assertEqual(report["after"], after)

    def test_widening_window_and_declaring_new_field(self):
        consumers = self.wide()
        consumers[1]["window"] = [1, 8]
        consumers[1]["reads_after"] = ["new", "a"]
        report = plan({"before": self.before, "after": self.after,
                       "consumers": consumers})
        check_plan(self.before, self.after, consumers, report)
        self.assertEqual(reference(self.before, self.after, consumers),
                         report_key(report))

    def test_plan_is_not_a_diff_with_post_validation(self):
        # A plain diff could drop "old" in stage 1; the plan must keep it until
        # the late consumer actually switches, and must not emit idle stages.
        before = ["a", "old", "b"]
        after = ["a", "new", "n2", "n3", "b"]
        consumers = [
            {"id": "fast", "reads_before": ["old"], "reads_after": ["new"],
             "window": [1, 1]},
            {"id": "slow", "reads_before": ["old"], "reads_after": ["new"],
             "window": [4, 8]},
        ]
        report = plan({"before": before, "after": after, "consumers": consumers})
        check_plan(before, after, consumers, report)
        first_drop = next(stage["stage"] for stage in report["plan"] if stage["drop"])
        self.assertEqual(first_drop, 4)
        self.assertIn("old", report["plan"][0]["fields"])
        self.assertEqual(reference(before, after, consumers), report_key(report))


# ---------------------------------------------------------------------------
# CLI contract


class Command(unittest.TestCase):
    def call(self, source, output, flagged=True):
        command = [sys.executable, "-m", "sequence_align.cli"]
        if flagged:
            command.append("--plan")
        command += ["--source", str(source), "--output", str(output)]
        return subprocess.run(command, capture_output=True, text=True)

    def write(self, directory, data, name="input.json"):
        path = Path(directory) / name
        path.write_text(json.dumps(data))
        return path

    def sample(self):
        return {
            "before": ["a", "old", "b"],
            "after": ["a", "new", "b"],
            "consumers": [
                {"id": "c0", "reads_before": ["old"], "reads_after": ["new"],
                 "window": [1, 8]},
            ],
        }

    def test_plan_cli_success_is_atomic_and_repeatable(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.write(directory, self.sample())
            output = Path(directory) / "report.json"
            result = self.call(source, output)
            self.assertEqual(result.returncode, 0, result.stderr)
            first = output.read_bytes()
            report = json.loads(first)
            self.assertEqual(report["status"], "satisfiable")
            check_plan(self.sample()["before"], self.sample()["after"],
                       self.sample()["consumers"], report)
            self.assertEqual(self.call(source, output).returncode, 0)
            self.assertEqual(first, output.read_bytes())
            self.assertEqual(json.loads(result.stdout), report)
            self.assertEqual(list(report), ["after", "plan", "reason", "status"])

    def test_unsatisfiable_writes_atomically_with_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            data = {
                "before": ["old", "s1", "s2", "s3"],
                "after": ["new", "s1", "s2", "s3", "n2", "n3"],
                "consumers": [
                    {"id": "fast", "reads_before": ["old"],
                     "reads_after": ["new"], "window": [1, 1]},
                    {"id": "slow", "reads_before": ["old"],
                     "reads_after": [], "window": [5, 8]},
                ],
            }
            source = self.write(directory, data)
            output = Path(directory) / "report.json"
            result = self.call(source, output)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_bytes())
            self.assertEqual(report["status"], "unsatisfiable")
            self.assertIsNone(report["plan"])
            self.assertIsInstance(report["reason"], dict)

    def test_invalid_input_exits_two_and_preserves_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            output.write_text("prior")
            invalid = [
                "{}",
                '{"before": ["a"], "after": ["b"]}',
                '{"before": ["a", "a"], "after": ["b"], "consumers": []}',
                '{"before": ["a"], "after": ["b"], "consumers": '
                '[{"id": "x", "reads_before": [], "reads_after": [], "window": [0, 1]}]}',
                '{"before": ["a"], "after": ["b"], "consumers": '
                '[{"id": "x", "reads_before": ["q"], "reads_after": [], "window": [1, 2]}]}',
            ]
            for body in invalid:
                source = Path(directory) / "bad.json"
                source.write_text(body)
                result = self.call(source, output)
                self.assertEqual(result.returncode, 2, (body, result.stderr))
                self.assertEqual(output.read_text(), "prior")

    def test_same_path_and_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.write(directory, self.sample())
            original = source.read_bytes()
            self.assertEqual(self.call(source, source).returncode, 2)
            self.assertEqual(source.read_bytes(), original)
            link = Path(directory) / "link.json"
            link.symlink_to(source)
            self.assertEqual(self.call(source, link).returncode, 2)
            self.assertEqual(source.read_bytes(), original)

    def test_legacy_entry_point_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            source = ROOT / "fixtures/input.json"
            output = Path(directory) / "report.json"
            result = self.call(source, output, flagged=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_bytes())
            self.assertEqual(set(report), {"schema_version", "source_sha256", "result"})
            self.assertEqual(report["schema_version"], 1)
            self.assertEqual(report["result"]["distance"], 2)
            self.assertEqual(report["result"]["counts"],
                             {"match": 2, "replace": 1, "delete": 0, "insert": 1})
            # Legacy repeated-token input stays valid; the new entry rejects it.
            legacy = Path(directory) / "legacy.json"
            legacy.write_text(json.dumps({"before": ["a", "a"], "after": ["a"]}))
            self.assertEqual(self.call(legacy, output, flagged=False).returncode, 0)
            self.assertEqual(self.call(legacy, output).returncode, 2)
            self.assertEqual(
                json.loads(output.read_bytes())["result"]["distance"], 1
            )


if __name__ == "__main__":
    unittest.main()
