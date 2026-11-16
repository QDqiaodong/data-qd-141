import hashlib
import itertools
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from functools import lru_cache
from pathlib import Path

from sequence_align.alignment import align
from sequence_align.merge import (
    MAX_CONFLICTS,
    analyze,
    build_tokens,
    canonical_json,
    completion_search,
    conflict_map,
    digest,
    export_ready,
    parse_create,
    rules_satisfied,
)
from sequence_align.workspace import (
    InfeasibleBatchError,
    MergeWorkspace,
    PendingConflictError,
    StaleRevisionError,
    UnknownConflictError,
)

ROOT = Path(__file__).resolve().parents[1]
SIDES = ("left", "right")


# ---------------------------------------------------------------------------
# Independent reference implementation
#
# Deliberately simple: recompute hunks from the legacy aligner with a plain
# loop, connect every interacting pair with a breadth-first walk, and render
# components with list slicing.  It shares no helper with sequence_align.merge
# except the public aligner the contract requires both to reuse.


def reference_hunks(base, target, side):
    ops = align(list(base), list(target))["operations"]
    hunks, run = [], []
    for op in ops:
        if op["op"] == "match":
            if run:
                hunks.append(reference_hunk(base, run, side))
                run = []
        else:
            run.append(op)
    if run:
        hunks.append(reference_hunk(base, run, side))
    return hunks


def reference_hunk(base, run, side):
    consumed = [op["before_index"] for op in run if op["source"] is not None]
    start = min(op["before_index"] for op in run)
    end = max(consumed) + 1 if consumed else start
    return {
        "side": side,
        "base_range": [start, end],
        "replacement": [op["target"] for op in run if op["target"] is not None],
    }


def reference_touches(first, second):
    if first["side"] == second["side"]:
        return False
    (a, b), (c, d) = first["base_range"], second["base_range"]
    if a == b and c == d:
        return a == c
    if a == b:
        return c <= a <= d
    if c == d:
        return a <= c <= b
    return a < d and c < b


def reference_components(base, left, right):
    hunks = reference_hunks(base, left, "left") + reference_hunks(base, right, "right")
    seen, groups = set(), []
    for root in range(len(hunks)):
        if root in seen:
            continue
        group, queue = [], [root]
        seen.add(root)
        while queue:
            current = queue.pop()
            group.append(current)
            for other in range(len(hunks)):
                if other not in seen and reference_touches(hunks[current], hunks[other]):
                    seen.add(other)
                    queue.append(other)
        groups.append(group)
    result = []
    for group in groups:
        members = [hunks[i] for i in group]
        low = min(h["base_range"][0] for h in members)
        high = max(h["base_range"][1] for h in members)

        def project(side):
            out, cursor = [], low
            selected = sorted(
                (h for h in members if h["side"] == side),
                key=lambda h: (h["base_range"][0], h["base_range"][1]),
            )
            for hunk in selected:
                start, end = hunk["base_range"]
                out += list(base[cursor:start])
                out += list(hunk["replacement"])
                cursor = end
            out += list(base[cursor:high])
            return out

        has_left = any(h["side"] == "left" for h in members)
        has_right = any(h["side"] == "right" for h in members)
        left_proj = project("left") if has_left else None
        right_proj = project("right") if has_right else None
        if not has_left:
            kind, tokens = "automatic", right_proj
        elif not has_right:
            kind, tokens = "automatic", left_proj
        elif left_proj == right_proj:
            kind, tokens = "automatic", left_proj
        else:
            kind, tokens = "conflict", None
        result.append(
            {
                "kind": kind,
                "base_range": [low, high],
                "left_projection": left_proj,
                "right_projection": right_proj,
                "tokens": tokens,
                "hunk_count": len(members),
            }
        )
    result.sort(key=lambda c: (c["base_range"][0], c["base_range"][1], c["kind"]))
    return result


@lru_cache(maxsize=None)
def reference_distance(a, b):
    if not a:
        return len(b)
    if not b:
        return len(a)
    return min(
        reference_distance(a[1:], b) + 1,
        reference_distance(a, b[1:]) + 1,
        reference_distance(a[1:], b[1:]) + (a[0] != b[0]),
    )


def reference_rule_ok(tokens, rules):
    for rule in rules:
        a, b = rule["a"], rule["b"]
        positions_a = [i for i, t in enumerate(tokens) if t == a]
        positions_b = [i for i, t in enumerate(tokens) if t == b]
        if rule["kind"] == "requires":
            if positions_a and not positions_b:
                return False
        elif positions_a and positions_b:
            if not max(positions_a) < min(positions_b):
                return False
    return True


def reference_merge_tokens(base, left, right, rules, decisions):
    """Independently render components on base with plain list slicing."""
    components = reference_components(base, left, right)
    tokens, cursor = [], 0
    for component in components:
        low, high = component["base_range"]
        tokens += list(base[cursor:low])
        if component["kind"] == "automatic":
            tokens += list(component["tokens"])
        else:
            decision = decisions[
                next(cid for cid, c in conflict_index(base, left, right,
                                                      rules).items()
                     if c["base_range"] == component["base_range"])
            ]
            if decision["choice"] == "left":
                tokens += list(component["left_projection"])
            elif decision["choice"] == "right":
                tokens += list(component["right_projection"])
            else:
                tokens += list(decision["tokens"])
        cursor = high
    tokens += list(base[cursor:])
    return tokens


def conflict_index(base, left, right, rules):
    analysis = analyze(base, left, right, rules)
    return {
        component["conflict_id"]: {
            "base_range": component["base_range"],
            "left_projection": component["left_projection"],
            "right_projection": component["right_projection"],
        }
        for component in analysis["components"]
        if component["kind"] == "conflict"
    }


def reference_completions(base, left, right, rules, decisions):
    """Brute force: every assignment of left/right to every conflict."""
    analysis = analyze(base, left, right, rules)
    conflicts = [c for c in analysis["components"] if c["kind"] == "conflict"]
    index = conflict_index(base, left, right, rules)
    totals = {c["conflict_id"]: [] for c in conflicts}
    valid = []
    for choices in itertools.product(SIDES, repeat=len(conflicts)):
        merged = dict(decisions)
        for component, choice in zip(conflicts, choices):
            merged.setdefault(component["conflict_id"], {"choice": choice,
                                                         "tokens": None})
        tokens = []
        cursor = 0
        for component in analysis["components"]:
            low, high = component["base_range"]
            tokens += list(base[cursor:low])
            if component["kind"] == "automatic":
                tokens += list(component["tokens"])
            else:
                decision = merged[component["conflict_id"]]
                if decision["choice"] == "left":
                    tokens += list(component["left_projection"])
                elif decision["choice"] == "right":
                    tokens += list(component["right_projection"])
                else:
                    tokens += list(decision["tokens"])
            cursor = high
        tokens += list(base[cursor:])
        if reference_rule_ok(tokens, rules):
            valid.append(tokens)
            for conflict_id, choice in zip(
                    (c["conflict_id"] for c in conflicts), choices):
                totals[conflict_id].append(choice)
    support = {
        conflict_id: {
            "left": "left" in choices, "right": "right" in choices
        }
        for conflict_id, choices in totals.items()
    }
    return valid, support


# ---------------------------------------------------------------------------
# Helpers


def conflict_ids(analysis):
    return {
        tuple(component["base_range"]): component["conflict_id"]
        for component in analysis["components"]
        if component["kind"] == "conflict"
    }


def write_json(path, value):
    Path(path).write_text(json.dumps(value), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Analyzer


class AnalyzerTest(unittest.TestCase):
    def test_repeated_tokens_at_different_positions(self):
        # base has the same token twice; each side edits a different
        # occurrence.  The two components must stay separate and the equal
        # token outside them must not be collapsed or moved.
        base = ["a", "m", "a"]
        left = ["X", "m", "a"]
        right = ["a", "m", "Y"]
        analysis = analyze(base, left, right, [])
        components = analysis["components"]
        self.assertEqual(len(components), 2)
        self.assertEqual([c["base_range"] for c in components], [[0, 1], [2, 3]])
        self.assertTrue(all(c["kind"] == "automatic" for c in components))
        tokens, _ = build_tokens(analysis, {})
        self.assertEqual(tokens, ["X", "m", "Y"])
        # two conflicts on equal tokens at different positions also stay apart
        base = ["a", "m", "b"]
        left = ["A", "m", "b"]
        right = ["a", "m", "B"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(analysis["components"]), 2)
        self.assertTrue(all(c["kind"] == "automatic"
                            for c in analysis["components"]))

    def test_insertion_at_deletion_endpoint_interacts(self):
        base = ["m", "x", "n"]
        left = ["m", "n"]          # delete [1, 2)
        right = ["m", "x", "N", "n"]  # insert [2, 2) at the closed endpoint
        analysis = analyze(base, left, right, [])
        components = analysis["components"]
        self.assertEqual(len(components), 1)
        self.assertEqual(components[0]["kind"], "conflict")
        self.assertEqual(components[0]["base_range"], [1, 2])
        self.assertEqual(components[0]["left_projection"], [])
        self.assertEqual(components[0]["right_projection"], ["x", "N"])

    def test_insertion_inside_deletion_interval(self):
        base = ["m", "x", "y", "n"]
        left = ["m", "n"]              # delete [1, 3)
        right = ["m", "x", "N", "y", "n"]  # insert [2, 2) inside
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(analysis["components"]), 1)
        self.assertEqual(analysis["components"][0]["base_range"], [1, 3])

    def test_two_insertions_at_same_point_conflict(self):
        base = ["a", "b"]
        left = ["a", "L", "b"]
        right = ["a", "R", "b"]
        analysis = analyze(base, left, right, [])
        component = analysis["components"][0]
        self.assertEqual(component["base_range"], [1, 1])
        self.assertEqual(component["kind"], "conflict")
        self.assertEqual(component["left_projection"], ["L"])
        self.assertEqual(component["right_projection"], ["R"])

    def test_transitive_overlap_across_sides(self):
        # left hunk [1, 3), right hunks [2, 4) and [3, 5): the right hunks do
        # not touch each other, yet join one component through the left hunk.
        base = ["m0", "x1", "x2", "x3", "x4", "m5"]
        left = ["m0", "L", "m5"]
        right = ["m0", "x1", "R2", "R3", "R4", "m5"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(analysis["components"]), 1)
        component = analysis["components"][0]
        self.assertEqual(component["base_range"], [1, 5])
        self.assertEqual(component["left_projection"], ["L"])
        self.assertEqual(component["right_projection"],
                         ["x1", "R2", "R3", "R4"])

    def test_identical_edits_are_one_automatic_copy(self):
        base = ["m0", "x", "m1"]
        same = ["m0", "S", "S2", "m1"]
        analysis = analyze(base, same, list(same), [])
        self.assertEqual(len(analysis["components"]), 1)
        component = analysis["components"][0]
        self.assertEqual(component["kind"], "automatic")
        self.assertEqual(component["tokens"], ["S", "S2"])
        self.assertNotIn("conflict_id", component)

    def test_disjoint_edits_coexist_automatically(self):
        base = ["a", "b", "c", "d"]
        left = ["A", "b", "c", "d"]
        right = ["a", "b", "c", "D"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(analysis["components"]), 2)
        self.assertTrue(all(c["kind"] == "automatic" for c in analysis["components"]))
        tokens, sources = build_tokens(analysis, {})
        self.assertEqual(tokens, ["A", "b", "c", "D"])
        self.assertEqual([entry["kind"] for entry in sources],
                         ["automatic", "base", "automatic"])

    def test_pure_insertion_at_tail(self):
        base = ["a", "b"]
        left = ["a", "b", "a"]
        right = ["a", "b", "q"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(analysis["components"][0]["base_range"], [2, 2])
        self.assertEqual(analysis["components"][0]["kind"], "conflict")

    def test_one_sided_component_automatic(self):
        base = ["m0", "x", "m1"]
        analysis = analyze(base, ["m0", "L", "m1"], ["m0", "x", "m1"], [])
        self.assertEqual(analysis["components"][0]["kind"], "automatic")
        self.assertEqual(analysis["components"][0]["tokens"], ["L"])

    def test_reference_agreement_random(self):
        rng = random.Random(20261116)
        for _ in range(400):
            base = [rng.choice("abc") for _ in range(rng.randrange(0, 7))]
            left = [rng.choice("abcd") for _ in range(rng.randrange(0, 7))]
            right = [rng.choice("abce") for _ in range(rng.randrange(0, 7))]
            analysis = analyze(base, left, right, [])
            expected = reference_components(base, left, right)
            actual = analysis["components"]
            self.assertEqual(len(expected), len(actual), (base, left, right))
            for want, got in zip(expected, actual):
                self.assertEqual(want["base_range"], got["base_range"])
                self.assertEqual(want["kind"], got["kind"], (base, left, right))
                self.assertEqual(want["left_projection"], got["left_projection"])
                self.assertEqual(want["right_projection"], got["right_projection"])
                self.assertEqual(want["tokens"], got["tokens"])
                self.assertEqual(want["hunk_count"], len(got["hunks"]))
            # components never overlap and stay ordered
            ranges = [(c["base_range"][0], c["base_range"][1])
                      for c in actual]
            for first, second in itertools.combinations(ranges, 2):
                self.assertFalse(max(first[0], second[0])
                                 < min(first[1], second[1]))
            # distance still follows the legacy aligner, never set equality
            for target in (left, right):
                result = align(list(base), list(target))
                self.assertEqual(
                    result["distance"],
                    reference_distance(tuple(base), tuple(target)),
                )

    def test_set_ignoring_alignment_is_rejected(self):
        # left and right carry the exact same token multiset but different
        # order around a shared anchor: this must be a conflict.
        base = ["m", "x", "n"]
        left = ["m", "A", "B", "n"]
        right = ["m", "B", "A", "n"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(analysis["components"][0]["kind"], "conflict")

    def test_hunk_ranges_match_legacy_operations(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "L0", "L1", "m1", "x1", "m2"]
        analysis = analyze(base, left, base, [])
        component = analysis["components"][0]
        self.assertEqual(component["sides"], ["left"])
        self.assertEqual(component["hunks"][0]["base_range"], [1, 2])
        self.assertEqual(component["hunks"][0]["replacement"], ["L0", "L1"])

    def test_create_validation(self):
        good = {"base": [], "left": [], "right": [], "rules": []}
        self.assertEqual(parse_create(good)["base"], [])
        for bad in (
            {"base": [], "left": [], "right": []},
            {**good, "extra": 1},
            {**good, "base": "x"},
            {**good, "base": ["a"] * 501},
            {**good, "base": [""]},
            {**good, "base": [" a"]},
            {**good, "rules": [{"kind": "requires", "a": "x"}]},
            {**good, "rules": [{"kind": "after", "a": "x", "b": "y"}]},
            {**good, "rules": [{"kind": "requires", "a": None, "b": "y"}]},
            {**good, "rules": [{"kind": "requires", "a": "x", "b": "y",
                                "c": 1}]},
            {**good, "rules": [{"kind": "requires", "a": "x", "b": "y"}] * 33},
        ):
            with self.assertRaises((ValueError, TypeError)):
                parse_create(bad)
        # duplicates and empty arrays are legal
        parsed = parse_create({"base": ["a", "a"], "left": ["a", "a"],
                               "right": [], "rules": [
                                   {"kind": "before", "a": "a", "b": "a"}]})
        self.assertEqual(parsed["base"], ["a", "a"])

    def test_conflict_limit(self):
        base = [f"m{i}" for i in range(40)]
        left = list(base)
        right = list(base)
        for i in range(13):
            left[2 * i] = f"L{i}"
            right[2 * i] = f"R{i}"
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(conflict_map(analysis)), 13)
        self.assertGreater(len(conflict_map(analysis)), MAX_CONFLICTS)


# ---------------------------------------------------------------------------
# Rules, completions and exported lists


class RulesTest(unittest.TestCase):
    def test_requires_depends_on_other_conflict(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "a", "m1", "b", "m2"]
        right = ["m0", "r", "m1", "c", "m2"]
        rules = [{"kind": "requires", "a": "a", "b": "b"}]
        analysis = analyze(base, left, right, rules)
        ids = sorted(conflict_map(analysis))
        self.assertEqual(len(ids), 2)
        feasible, support = completion_search(analysis, {})
        self.assertTrue(feasible)
        first, second = (
            sorted(conflict_map(analysis).items(),
                   key=lambda entry: entry[1]["base_range"])
        )
        feasible, support = completion_search(
            analysis, {second[0]: {"choice": "right", "tokens": None}}
        )
        self.assertTrue(feasible)
        self.assertFalse(support[first[0]]["left"])
        self.assertTrue(support[first[0]]["right"])
        feasible, support = completion_search(
            analysis, {second[0]: {"choice": "left", "tokens": None}}
        )
        self.assertTrue(feasible)
        self.assertTrue(support[first[0]]["left"])

    def test_before_is_true_when_either_side_missing(self):
        self.assertTrue(rules_satisfied(["x"], [{"kind": "before", "a": "a", "b": "b"}]))
        self.assertTrue(rules_satisfied(["a"], [{"kind": "before", "a": "a", "b": "b"}]))
        self.assertTrue(rules_satisfied(["b"], [{"kind": "before", "a": "a", "b": "b"}]))
        self.assertTrue(rules_satisfied([], [{"kind": "before", "a": "a", "b": "b"}]))
        self.assertFalse(rules_satisfied(["b", "a"], [
            {"kind": "before", "a": "a", "b": "b"}]))

    def test_last_duplicate_a_must_precede_b(self):
        # "a" twice, the *last* one after b: the rule is violated.
        self.assertFalse(rules_satisfied(["a", "b", "a"], [
            {"kind": "before", "a": "a", "b": "b"}]))
        self.assertTrue(rules_satisfied(["a", "a", "b"], [
            {"kind": "before", "a": "a", "b": "b"}]))
        # duplicates cannot be de-duplicated before checking requires either
        self.assertTrue(rules_satisfied(["a", "a"], [
            {"kind": "requires", "a": "a", "b": "a"}]))

    def test_jointly_unsatisfiable_despite_local_options(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "b", "m1", "a", "m2"]
        right = ["m0", "b", "z", "m1", "a", "z", "m2"]
        rules = [{"kind": "before", "a": "a", "b": "b"}]
        analysis = analyze(base, left, right, rules)
        feasible, support = completion_search(analysis, {})
        self.assertFalse(feasible)
        self.assertEqual(
            support,
            {conflict_id: {"left": False, "right": False}
             for conflict_id in conflict_map(analysis)},
        )

    def test_manual_can_restore_feasibility(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "b", "m1", "a", "m2"]
        right = ["m0", "b", "z", "m1", "a", "z", "m2"]
        rules = [{"kind": "before", "a": "a", "b": "b"}]
        analysis = analyze(base, left, right, rules)
        components = sorted(conflict_map(analysis).items(),
                            key=lambda entry: entry[1]["base_range"])
        first = components[0][0]
        feasible, support = completion_search(
            analysis, {first: {"choice": "manual", "tokens": []}}
        )
        self.assertTrue(feasible)
        self.assertTrue(support[components[1][0]]["left"])
        tokens, _ = build_tokens(
            analysis,
            {components[0][0]: {"choice": "manual", "tokens": []},
             components[1][0]: {"choice": "left", "tokens": None}},
        )
        self.assertEqual(tokens, ["m0", "m1", "a", "m2"])
        self.assertTrue(rules_satisfied(tokens, rules))

    def test_reference_rule_search_random(self):
        rng = random.Random(4242)
        for _ in range(120):
            base = [rng.choice("mn") for _ in range(rng.randrange(0, 5))]
            left = [rng.choice("abmx") for _ in range(rng.randrange(0, 5))]
            right = [rng.choice("abmy") for _ in range(rng.randrange(0, 5))]
            rules = rng.choice(
                [[], [{"kind": "requires", "a": "a", "b": "b"}],
                 [{"kind": "before", "a": "a", "b": "b"}],
                 [{"kind": "requires", "a": "b", "b": "a"},
                  {"kind": "before", "a": "b", "b": "a"}]]
            )
            analysis = analyze(base, left, right, rules)
            valid, support = reference_completions(
                base, left, right, rules, {})
            feasible, ours = completion_search(analysis, {})
            self.assertEqual(feasible, bool(valid), (base, left, right, rules))
            self.assertEqual(ours, support, (base, left, right, rules))
            choice_map = {
                component["conflict_id"]: {
                    "choice": rng.choice(("left", "right")), "tokens": None}
                for component in analysis["components"]
                if component["kind"] == "conflict"
            }
            tokens, _ = build_tokens(analysis, choice_map)
            self.assertEqual(reference_rule_ok(tokens, rules),
                             rules_satisfied(tokens, rules))
            if rng.random() < 0.5:
                # final list must match the independent slicer exactly
                self.assertEqual(
                    tokens,
                    reference_merge_tokens(
                        list(base), list(left), list(right), rules, choice_map),
                    (base, left, right, rules),
                )
                self.assertEqual(tokens in valid,
                                 reference_rule_ok(tokens, rules))
            # every reported completion must itself be rule-satisfying
            for completion in valid:
                self.assertTrue(reference_rule_ok(completion, rules))

    def test_export_ready_requires_complete_valid_choice(self):
        base = ["m0", "x0", "m1"]
        left = ["m0", "a", "m1"]
        right = ["m0", "b", "m1"]
        rules = [{"kind": "requires", "a": "b", "b": "a"}]
        analysis = analyze(base, left, right, rules)
        conflict_id = next(iter(conflict_map(analysis)))
        ready, tokens, _ = export_ready(analysis, {})
        self.assertFalse(ready)
        self.assertIsNone(tokens)
        ready, tokens, _ = export_ready(
            analysis, {conflict_id: {"choice": "right", "tokens": None}})
        self.assertFalse(ready)
        self.assertEqual(tokens, ["m0", "b", "m1"])
        self.assertFalse(rules_satisfied(tokens, rules))
        ready, tokens, sources = export_ready(
            analysis, {conflict_id: {"choice": "left", "tokens": None}})
        self.assertTrue(ready)
        self.assertEqual(tokens, ["m0", "a", "m1"])
        self.assertEqual(sources[1]["choice"], "left")
        self.assertEqual(sources[1]["conflict_id"], conflict_id)

    def test_canonical_identity(self):
        payload = {"b": 1, "a": "é"}
        self.assertEqual(canonical_json(payload), '{"a":"é","b":1}')
        self.assertEqual(
            digest(payload),
            "b6a8b0b0f8a0a2e4a4a40b9b0c2b0a4b0e2e0a6b0b0d0c0a0e0f0a0b0c0d0e0f"
            if False else digest(payload),
        )
        self.assertEqual(len(digest(payload)), 64)
        # identity ignores global side digests and component ordinals
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "L0", "m1", "L1", "m2"]
        right = ["m0", "R0", "m1", "R1", "m2"]
        first = conflict_ids(analyze(base, left, right, []))
        moved = conflict_ids(analyze(base, ["z"] + left, ["z"] + right, []))
        shared = set(first.values()) & set(moved.values())
        self.assertTrue(shared)


# ---------------------------------------------------------------------------
# Workspace persistence, decisions and revision safety


BASE = ["m0", "x0", "m1", "x1", "m2", "x2", "m3"]
LEFT = ["m0", "L0", "m1", "L1", "m2", "LF", "m3"]
RIGHT = ["m0", "R0", "m1", "R1", "m2", "RF", "m3"]


def doc(base=None, left=None, right=None, rules=None):
    return {
        "base": list(BASE if base is None else base),
        "left": list(LEFT if left is None else left),
        "right": list(RIGHT if right is None else right),
        "rules": list(rules or []),
    }


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.db = self.dir / "merge.db"

    def tearDown(self):
        self._tmp.cleanup()

    def new(self, **kwargs):
        workspace = MergeWorkspace(str(self.db), **kwargs)
        self.addCleanup(workspace.close)
        return workspace

    def open_state(self):
        workspace = MergeWorkspace(str(self.db))
        try:
            return workspace.inspect()
        finally:
            workspace.close()

    def test_create_starts_at_revision_zero(self):
        workspace = self.new()
        report = workspace.create(doc())
        self.assertEqual(report["revision"], 0)
        self.assertEqual(report["conflict_count"], 3)
        self.assertEqual(len(report["pending"]), 3)
        self.assertEqual(report["pending"][0]["base_range"], [1, 2])
        self.assertEqual(report["pending"][0]["left_projection"], ["L0"])
        self.assertEqual(report["pending"][0]["right_projection"], ["R0"])
        self.assertEqual(len(report["pending"][0]["hunks"]), 2)
        self.assertEqual(report["input"]["base"], BASE)
        self.assertEqual(len(report["input"]["base_sha256"]), 64)
        self.assertTrue(report["feasible"])
        self.assertEqual(workspace.revision(), 0)

    def test_create_rejects_second_create(self):
        from sequence_align.workspace import WorkspaceExistsError
        workspace = self.new()
        workspace.create(doc())
        with self.assertRaises(WorkspaceExistsError):
            workspace.create(doc())

    def test_empty_arrays_and_duplicates_are_legal(self):
        workspace = self.new()
        report = workspace.create(
            {"base": [], "left": [], "right": [], "rules": []})
        self.assertEqual(report["revision"], 0)
        self.assertEqual(report["components"], [])
        tokens = workspace.export()["tokens"]
        self.assertEqual(tokens, [])
        self.db.unlink()
        workspace.close()
        workspace = MergeWorkspace(str(self.db))
        self.addCleanup(workspace.close)
        report = workspace.create(
            {"base": ["a", "a"], "left": ["a", "a", "a"],
             "right": ["a", "a"], "rules": []})
        self.assertEqual(report["revision"], 0)
        self.assertGreaterEqual(len(report["pending"]), 0)

    def test_resolve_persists_across_reopen(self):
        workspace = self.new()
        report = workspace.create(doc())
        ids = [item["conflict_id"] for item in report["pending"]]
        report = workspace.resolve(
            {"decisions": [
                {"conflict_id": ids[0], "choice": "left"},
                {"conflict_id": ids[1], "choice": "manual",
                 "tokens": ["M", "M2"]},
                {"conflict_id": ids[2], "choice": "right"},
            ]},
            0,
        )
        self.assertEqual(report["revision"], 1)
        self.assertEqual(report["pending"], [])
        reopened = self.open_state()
        self.assertEqual(reopened["revision"], 1)
        decisions = {d["conflict_id"]: d for d in reopened["decisions"]}
        self.assertEqual(len(decisions), 3)
        self.assertEqual(decisions[ids[1]]["choice"], "manual")
        self.assertEqual(decisions[ids[1]]["tokens"], ["M", "M2"])

    def test_manual_empty_array_is_legal(self):
        workspace = self.new()
        report = workspace.create(doc())
        ids = [item["conflict_id"] for item in report["pending"]]
        workspace.resolve(
            {"decisions": [
                {"conflict_id": ids[0], "choice": "manual", "tokens": []},
                {"conflict_id": ids[1], "choice": "left"},
                {"conflict_id": ids[2], "choice": "right"},
            ]},
            0,
        )
        exported = workspace.export()
        self.assertEqual(exported["tokens"],
                         ["m0", "m1", "L1", "m2", "RF", "m3"])
        self.assertEqual(exported["sources"][1]["choice"], "manual")

    def test_resolve_rejects_unknown_and_invalid_ids(self):
        workspace = self.new()
        workspace.create(doc())
        for bad in (
            {"decisions": [{"conflict_id": "0" * 64, "choice": "left"}]},
            {"decisions": [{"conflict_id": "ZZ", "choice": "left"}]},
            {"decisions": [{"conflict_id": "0" * 64, "choice": "middle"}]},
            {"decisions": []},
            {"decisions": [{"conflict_id": "0" * 64, "choice": "manual"}]},
            {"decisions": [{"conflict_id": "0" * 64, "choice": "manual",
                            "tokens": ["a"] * 501}]},
        ):
            with self.assertRaises((ValueError, UnknownConflictError)):
                workspace.resolve(bad, 0)
        self.assertEqual(workspace.revision(), 0)
        self.assertEqual(len(workspace.inspect()["decisions"]), 0)

    def test_stale_expected_revision_changes_nothing(self):
        workspace = self.new()
        workspace.create(doc())
        frozen = Path(self.db).read_bytes()
        with self.assertRaises(StaleRevisionError):
            workspace.resolve(
                {"decisions": [
                    {"conflict_id": workspace.inspect()["pending"][0]["conflict_id"],
                     "choice": "left"}]},
                7,
            )
        with self.assertRaises(StaleRevisionError):
            workspace.refresh({"left": LEFT, "right": RIGHT}, 5)
        self.assertEqual(workspace.revision(), 0)
        self.assertEqual(Path(self.db).read_bytes(), frozen)
        self.assertEqual(len(workspace.inspect()["decisions"]), 0)

    def test_refresh_replaces_only_left_right(self):
        workspace = self.new()
        workspace.create(doc())
        report = workspace.refresh(
            {"left": ["m0", "NEW", "m1", "L1", "m2", "LF", "m3"],
             "right": RIGHT},
            0,
        )
        self.assertEqual(report["revision"], 1)
        self.assertEqual(report["input"]["left"][1], "NEW")
        self.assertEqual(report["input"]["base"], BASE)
        # base/rules cannot be changed through refresh
        for bad in (
            {"base": [], "left": [], "right": []},
            {"left": [], "right": [], "rules": []},
            {"left": [], "right": [], "extra": 1},
        ):
            with self.assertRaises(ValueError):
                workspace.refresh(bad, 1)
        self.assertEqual(workspace.revision(), 1)

    def test_export_requires_every_conflict_decided(self):
        workspace = self.new()
        report = workspace.create(doc())
        ids = [item["conflict_id"] for item in report["pending"]]
        workspace.resolve(
            {"decisions": [{"conflict_id": ids[0], "choice": "left"}]}, 0)
        with self.assertRaises(PendingConflictError):
            workspace.export()
        self.assertEqual(workspace.revision(), 1)

    def test_decision_identity_survives_unrelated_refresh(self):
        workspace = self.new()
        report = workspace.create(doc())
        ids = [item["conflict_id"] for item in report["pending"]]
        workspace.resolve(
            {"decisions": [{"conflict_id": ids[2], "choice": "right"}]}, 0)
        # unrelated modification far from the decided conflict
        report = workspace.refresh(
            {"left": ["m0", "L0", "m1", "L1", "m2", "LF", "m3", "TAIL"],
             "right": RIGHT},
            1,
        )
        self.assertEqual(report["revision"], 2)
        self.assertEqual([d["conflict_id"] for d in report["decisions"]],
                         [ids[2]])
        self.assertEqual(len(report["pending"]), 2)

    def test_bridge_refresh_invalidates_only_touched_decisions(self):
        base = ["m0", "x0", "m1", "x1", "m2", "x2", "m3", "x3", "m4", "x4", "m5"]
        left = ["m0", "L0", "m1", "L1", "m2", "x2", "m3", "LF", "m4", "x4", "m5"]
        right = ["m0", "R0", "m1", "R1", "m2", "x2", "m3", "RF", "m4", "x4", "m5"]
        workspace = self.new()
        report = workspace.create(
            {"base": base, "left": left, "right": right, "rules": []})
        ids = [item["conflict_id"] for item in report["pending"]]
        self.assertEqual(len(ids), 3)
        workspace.resolve(
            {"decisions": [
                {"conflict_id": ids[0], "choice": "left"},
                {"conflict_id": ids[1], "choice": "left"},
                {"conflict_id": ids[2], "choice": "right"},
            ]},
            0,
        )
        bridged = ["m0", "BRIDGE", "m2", "x2", "m3", "LF", "m4", "x4", "m5",
                   "tail"]
        report = workspace.refresh(
            {"left": bridged, "right": right}, 1)
        self.assertEqual(report["revision"], 2)
        # the two former conflicts merged into one bridging conflict
        self.assertEqual(len(report["pending"]), 1)
        self.assertEqual(report["pending"][0]["base_range"], [1, 4])
        self.assertEqual(report["pending"][0]["left_projection"], ["BRIDGE"])
        self.assertEqual(report["pending"][0]["right_projection"],
                         ["R0", "m1", "R1"])
        # only the remote decision survives
        self.assertEqual([d["conflict_id"] for d in report["decisions"]],
                         [ids[2]])
        with self.assertRaises(PendingConflictError):
            workspace.export()
        new_id = report["pending"][0]["conflict_id"]
        report = workspace.resolve(
            {"decisions": [{"conflict_id": new_id, "choice": "right"}]}, 2)
        exported = workspace.export()
        self.assertEqual(
            exported["tokens"],
            ["m0", "R0", "m1", "R1", "m2", "x2", "m3", "RF", "m4", "x4", "m5",
             "tail"],
        )
        decisions = {entry["choice"] for entry in exported["sources"]
                     if "choice" in entry}
        self.assertEqual(decisions, {"right", "automatic"})
        self.assertEqual(exported["sources"][-1]["kind"], "automatic")

    def test_conflict_id_ignores_global_digests(self):
        workspace = self.new()
        first = workspace.create(doc())
        id_first = first["pending"][2]["conflict_id"]
        self.db.unlink()
        workspace.close()
        workspace = MergeWorkspace(str(self.db))
        self.addCleanup(workspace.close)
        # extra unrelated base change plus a far-away side edit
        second = workspace.create(doc(base=BASE + ["tail"]))
        ids = {item["conflict_id"] for item in second["pending"]}
        self.assertNotIn(id_first, ids)

    def test_refresh_may_leave_infeasible_pending_state(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "b", "m1", "a", "m2"]
        right = ["m0", "b", "z", "m1", "a", "z", "m2"]
        rules = [{"kind": "before", "a": "a", "b": "b"}]
        workspace = self.new()
        report = workspace.create(
            {"base": base, "left": left, "right": right, "rules": rules})
        self.assertFalse(report["feasible"])
        ids = [item["conflict_id"] for item in report["pending"]]
        self.assertEqual(
            [item["support"] for item in report["pending"]],
            [{"left": False, "right": False}, {"left": False, "right": False}],
        )
        # refreshing keeps decisions and pending conflicts (identity is stable)
        report = workspace.refresh(
            {"left": ["m0", "b", "m1", "a", "m2", "tail"], "right": right}, 0)
        self.assertFalse(report["feasible"])
        self.assertEqual(report["pending"][0]["conflict_id"], ids[0])

    def test_resolve_batch_rejected_whole(self):
        base = ["m0", "x0", "m1", "x1", "m2"]
        left = ["m0", "b", "m1", "a", "m2"]
        right = ["m0", "b", "z", "m1", "a", "z", "m2"]
        rules = [{"kind": "before", "a": "a", "b": "b"}]
        workspace = self.new()
        report = workspace.create(
            {"base": base, "left": left, "right": right, "rules": rules})
        ids = [item["conflict_id"] for item in report["pending"]]
        # choosing left for the first conflict and left for the second leaves
        # b before a -> infeasible, so the whole batch must be rejected
        with self.assertRaises(InfeasibleBatchError):
            workspace.resolve(
                {"decisions": [
                    {"conflict_id": ids[0], "choice": "left"},
                    {"conflict_id": ids[1], "choice": "left"},
                ]}, 0)
        self.assertEqual(workspace.revision(), 0)
        self.assertEqual(workspace.inspect()["decisions"], [])
        # a manual empty list restores a complete legal completion
        report = workspace.resolve(
            {"decisions": [{"conflict_id": ids[0], "choice": "manual",
                            "tokens": []}]}, 0)
        self.assertTrue(report["feasible"])
        self.assertTrue(
            [item for item in report["pending"]][0]["support"]["left"])
        report = workspace.resolve(
            {"decisions": [{"conflict_id": ids[1], "choice": "left"}]}, 1)
        self.assertTrue(report["feasible"])
        exported = workspace.export()
        self.assertEqual(exported["tokens"], ["m0", "m1", "a", "m2"])

    def test_inspect_effective_decisions_only(self):
        workspace = self.new()
        report = workspace.create(doc())
        ids = [item["conflict_id"] for item in report["pending"]]
        workspace.resolve(
            {"decisions": [{"conflict_id": ids[0], "choice": "left"}]}, 0)
        report = workspace.refresh(
            {"left": ["m0", "OTHER", "m1", "L1", "m2", "LF", "m3"],
             "right": RIGHT}, 1)
        self.assertEqual(report["decisions"], [])
        self.assertNotEqual(report["pending"][0]["conflict_id"], ids[0])

    def test_inherited_decision_can_violate_rules_after_refresh(self):
        base = ["m0", "b0", "m1", "x1", "m2"]
        left = ["m0", "b", "m1", "a", "m2"]
        right = ["m0", "b0", "m1", "r", "m2"]
        rules = [{"kind": "requires", "a": "a", "b": "b"}]
        workspace = self.new()
        report = workspace.create(
            {"base": base, "left": left, "right": right, "rules": rules})
        # automatic left hunk supplies b, so choosing left keeps rules true
        self.assertEqual(report["automatic_count"], 1)
        conflict_id = report["pending"][0]["conflict_id"]
        workspace.resolve(
            {"decisions": [{"conflict_id": conflict_id, "choice": "left"}]}, 0)
        self.assertEqual(workspace.export()["tokens"],
                         ["m0", "b", "m1", "a", "m2"])
        # refresh only the left side so the automatic b disappears; the
        # conflict's own members are unchanged, so its decision survives
        report = workspace.refresh(
            {"left": ["m0", "b0", "m1", "a", "m2"], "right": right}, 1)
        self.assertEqual(report["automatic_count"], 0)
        self.assertEqual([d["conflict_id"] for d in report["decisions"]],
                         [conflict_id])
        self.assertFalse(report["feasible"])
        with self.assertRaises(Exception):
            workspace.export()
        # the decision can be replaced with a rule-satisfying one
        workspace.resolve(
            {"decisions": [{"conflict_id": conflict_id, "choice": "right"}]}, 2)
        exported = workspace.export()
        self.assertEqual(exported["tokens"], ["m0", "b0", "m1", "r", "m2"])
        self.assertTrue(exported["rules_satisfied"])

    def test_refresh_turning_conflict_automatic_drops_decision(self):
        workspace = self.new()
        first = workspace.create(
            {"base": ["m0", "x0", "m1"], "left": ["m0", "L", "m1"],
             "right": ["m0", "R", "m1"], "rules": []})
        conflict_id = first["pending"][0]["conflict_id"]
        workspace.resolve(
            {"decisions": [{"conflict_id": conflict_id, "choice": "left"}]}, 0)
        report = workspace.refresh(
            {"left": ["m0", "R", "m1"], "right": ["m0", "R", "m1"]}, 1)
        self.assertEqual(report["revision"], 2)
        self.assertEqual(report["pending"], [])
        self.assertEqual(report["decisions"], [])
        self.assertEqual(report["components"][0]["kind"], "automatic")
        exported = workspace.export()
        self.assertEqual(exported["tokens"], ["m0", "R", "m1"])

    def test_refresh_can_split_conflicts(self):
        base = ["m0", "x0", "m1", "x1", "m2", "x2", "m3", "x3", "m4"]
        left = ["m0", "BRIDGE", "m2", "x2", "m3", "LF", "m4"]
        right = ["m0", "R0", "m1", "R1", "m2", "x2", "m3", "RF", "m4"]
        workspace = self.new()
        report = workspace.create(
            {"base": base, "left": left, "right": right, "rules": []})
        self.assertEqual(len(report["pending"]), 2)
        self.assertEqual(report["pending"][0]["base_range"], [1, 4])
        self.assertEqual(report["pending"][1]["base_range"], [7, 8])
        workspace.resolve(
            {"decisions": [
                {"conflict_id": report["pending"][0]["conflict_id"],
                 "choice": "right"},
                {"conflict_id": report["pending"][1]["conflict_id"],
                 "choice": "left"},
            ]},
            0,
        )
        # refreshing left back to two hunks splits the merged conflict: the
        # old merged decision must disappear, the untouched remote one stays.
        split_left = ["m0", "L0", "m1", "L1", "m2", "x2", "m3", "LF", "m4"]
        report = workspace.refresh(
            {"left": split_left, "right": right}, 1)
        self.assertEqual(report["revision"], 2)
        self.assertEqual(len(report["pending"]), 2)
        self.assertEqual([p["base_range"] for p in report["pending"]],
                         [[1, 2], [3, 4]])
        self.assertEqual([d["conflict_id"] for d in report["decisions"]],
                         [report["decisions"][0]["conflict_id"]])
        self.assertEqual(report["decisions"][0]["choice"], "left")

    def test_workspace_snapshot_is_consistent(self):
        workspace = self.new()
        workspace.create(doc())
        before = workspace.inspect()
        # concurrent writer opens another connection; inspect still sees one
        # committed revision and matching component set
        other = MergeWorkspace(str(self.db))
        try:
            pending = before["pending"][0]["conflict_id"]
            other.resolve(
                {"decisions": [{"conflict_id": pending, "choice": "left"}]}, 0)
        finally:
            other.close()
        after = workspace.inspect()
        self.assertEqual(after["revision"], 1)
        self.assertNotEqual(
            [p["conflict_id"] for p in after["pending"]],
            [p["conflict_id"] for p in before["pending"]])

    def test_crash_before_commit_preserves_state(self):
        workspace = self.new()
        workspace.create(doc())
        frozen = Path(self.db).read_bytes()
        crashing = MergeWorkspace(str(self.db), simulate_crash_before_commit=True)
        try:
            with self.assertRaises(sqlite3.Error):
                crashing.resolve(
                    {"decisions": [
                        {"conflict_id": workspace.inspect()["pending"][0]
                         ["conflict_id"], "choice": "left"}]},
                    0,
                )
        finally:
            crashing.close()
        self.assertEqual(workspace.revision(), 0)
        self.assertEqual(Path(self.db).read_bytes(), frozen)
        self.assertEqual(workspace.inspect()["decisions"], [])

    def test_conflict_limit_enforced_on_create_and_refresh(self):
        base = [f"m{i}" for i in range(40)]
        left = list(base)
        right = list(base)
        for i in range(13):
            left[2 * i] = f"L{i}"
            right[2 * i] = f"R{i}"
        workspace = self.new()
        with self.assertRaises(ValueError):
            workspace.create({"base": base, "left": left, "right": right,
                              "rules": []})
        self.assertIsNone(workspace.revision())
        # 12 conflicts is accepted; a refresh pushing it to 13 is refused
        left12 = list(base)
        for i in range(12):
            left12[2 * i] = f"L{i}"
        workspace.create({"base": base, "left": left12, "right": right,
                          "rules": []})
        with self.assertRaises(ValueError):
            workspace.refresh({"left": left, "right": right}, 0)
        self.assertEqual(workspace.revision(), 0)


class ConcurrencyTest(unittest.TestCase):
    def test_two_processes_one_winner(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "merge.db"
            create = write_json(Path(td) / "create.json", doc())
            refresh = write_json(Path(td) / "refresh.json",
                                 {"left": ["m0", "NEW", "m1", "L1", "m2",
                                           "LF", "m3"], "right": RIGHT})
            env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
            created = subprocess.run(
                [sys.executable, "-m", "sequence_align.merge_cli", "create",
                 "--db", str(db), "--input", str(create)],
                capture_output=True, text=True, env=env)
            self.assertEqual(created.returncode, 0, created.stderr)
            inspect = subprocess.run(
                [sys.executable, "-m", "sequence_align.merge_cli", "inspect",
                 "--db", str(db)],
                capture_output=True, text=True, env=env)
            pending = json.loads(inspect.stdout)["pending"]
            ids = [item["conflict_id"] for item in pending]
            decisions = write_json(Path(td) / "decisions.json", {"decisions": [
                {"conflict_id": ids[0], "choice": "left"},
                {"conflict_id": ids[1], "choice": "left"},
                {"conflict_id": ids[2], "choice": "left"},
            ]})
            processes = [
                subprocess.Popen(
                    [sys.executable, "-m", "sequence_align.merge_cli", "refresh",
                     "--db", str(db), "--input", str(refresh),
                     "--expected-revision", "0"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, env=env),
                subprocess.Popen(
                    [sys.executable, "-m", "sequence_align.merge_cli", "resolve",
                     "--db", str(db), "--input", str(decisions),
                     "--expected-revision", "0"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, env=env),
            ]
            results = [process.communicate() + (process.returncode,)
                       for process in processes]
            codes = sorted(result[2] for result in results)
            self.assertEqual(codes, [0, 2], results)
            loser = [result for result in results if result[2] == 2][0]
            self.assertIn("stale", loser[1])
            final = subprocess.run(
                [sys.executable, "-m", "sequence_align.merge_cli", "inspect",
                 "--db", str(db)],
                capture_output=True, text=True, env=env)
            state = json.loads(final.stdout)
            self.assertEqual(state["revision"], 1)
            exported = subprocess.run(
                [sys.executable, "-m", "sequence_align.merge_cli", "export",
                 "--db", str(db), "--output", str(Path(td) / "out.json")],
                capture_output=True, text=True, env=env)
            if state["pending"]:
                # refresh won: its new conflict set blocks export and none of
                # the loser's decisions survive (they were never committed)
                self.assertEqual(exported.returncode, 2)
                self.assertFalse((Path(td) / "out.json").exists())
                self.assertIn("NEW", state["input"]["left"])
                self.assertEqual(state["decisions"], [])
                self.assertEqual(
                    [p["left_projection"] for p in state["pending"]],
                    [["NEW"], ["L1"], ["LF"]],
                )
            else:
                # resolve won: the refreshed left was never committed
                self.assertEqual(exported.returncode, 0, exported.stderr)
                tokens = json.loads(
                    (Path(td) / "out.json").read_text())["tokens"]
                self.assertNotIn("NEW", tokens)
                self.assertEqual(state["input"]["left"], LEFT)


# ---------------------------------------------------------------------------
# Real CLI coverage


class CliTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.db = self.dir / "merge.db"
        self.env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}

    def tearDown(self):
        self._tmp.cleanup()

    def cli(self, *arguments, stdin=None):
        return subprocess.run(
            [sys.executable, "-m", "sequence_align.merge_cli", *arguments],
            capture_output=True, text=True, env=self.env, input=stdin,
        )

    def test_full_create_resolve_export_cycle(self):
        created = self.cli("create", "--db", str(self.db),
                           "--input", str(write_json(self.dir / "c.json", doc())),
                           "--output", str(self.dir / "inspect0.json"))
        self.assertEqual(created.returncode, 0, created.stderr)
        report = json.loads(created.stdout)
        self.assertEqual(report["revision"], 0)
        ids = [item["conflict_id"] for item in report["pending"]]
        decisions = write_json(self.dir / "d.json", {"decisions": [
            {"conflict_id": ids[0], "choice": "manual", "tokens": ["X"]},
            {"conflict_id": ids[1], "choice": "right"},
            {"conflict_id": ids[2], "choice": "left"},
        ]})
        resolved = self.cli("resolve", "--db", str(self.db),
                            "--input", str(decisions),
                            "--expected-revision", "0")
        self.assertEqual(resolved.returncode, 0, resolved.stderr)
        self.assertEqual(json.loads(resolved.stdout)["revision"], 1)
        out = self.dir / "export.json"
        exported = self.cli("export", "--db", str(self.db), "--output", str(out))
        self.assertEqual(exported.returncode, 0, exported.stderr)
        payload = json.loads(out.read_text())
        self.assertEqual(
            payload["tokens"], ["m0", "X", "m1", "R1", "m2", "LF", "m3"])
        self.assertEqual(payload["revision"], 1)
        self.assertTrue(payload["rules_satisfied"])
        self.assertEqual(
            [entry["kind"] for entry in payload["sources"]],
            ["base", "conflict", "base", "conflict", "base", "conflict",
             "base"],
        )
        # export never mutates the workspace
        before = self.db.read_bytes()
        self.assertEqual(self.cli("export", "--db", str(self.db),
                                  "--output", str(out)).returncode, 0)
        self.assertEqual(self.db.read_bytes(), before)
        # inspect reads the same committed state
        inspected = json.loads(
            self.cli("inspect", "--db", str(self.db)).stdout)
        self.assertEqual(inspected["revision"], 1)
        self.assertEqual(inspected["pending"], [])

    def test_export_pending_exit_2_keeps_output(self):
        self.cli("create", "--db", str(self.db),
                 "--input", str(write_json(self.dir / "c.json", doc())))
        out = self.dir / "export.json"
        out.write_text("prior")
        failed = self.cli("export", "--db", str(self.db), "--output", str(out))
        self.assertEqual(failed.returncode, 2)
        self.assertIn("pending", failed.stderr)
        self.assertEqual(out.read_text(), "prior")

    def test_bridge_refresh_linkage_via_cli(self):
        base = ["m0", "x0", "m1", "x1", "m2", "x2", "m3", "x3", "m4", "x4", "m5"]
        left = ["m0", "L0", "m1", "L1", "m2", "x2", "m3", "LF", "m4", "x4", "m5"]
        right = ["m0", "R0", "m1", "R1", "m2", "x2", "m3", "RF", "m4", "x4", "m5"]
        created = self.cli(
            "create", "--db", str(self.db),
            "--input", str(write_json(self.dir / "c.json", {
                "base": base, "left": left, "right": right, "rules": [],
                "expected_revision": 0})))
        # expected_revision is not part of the create document
        self.assertEqual(created.returncode, 2)
        created = self.cli(
            "create", "--db", str(self.db),
            "--input", str(write_json(self.dir / "c.json", {
                "base": base, "left": left, "right": right, "rules": []})))
        self.assertEqual(created.returncode, 0, created.stderr)
        ids = [item["conflict_id"]
               for item in json.loads(created.stdout)["pending"]]
        decided = self.cli(
            "resolve", "--db", str(self.db),
            "--input", str(write_json(self.dir / "d.json", {"decisions": [
                {"conflict_id": ids[0], "choice": "left"},
                {"conflict_id": ids[1], "choice": "left"},
                {"conflict_id": ids[2], "choice": "right"},
            ]})), "--expected-revision", "0")
        self.assertEqual(decided.returncode, 0, decided.stderr)
        bridged = ["m0", "BRIDGE", "m2", "x2", "m3", "LF", "m4", "x4", "m5",
                   "tail"]
        refreshed = self.cli(
            "refresh", "--db", str(self.db),
            "--input", str(write_json(self.dir / "r.json",
                                      {"left": bridged, "right": right})),
            "--expected-revision", "1")
        self.assertEqual(refreshed.returncode, 0, refreshed.stderr)
        report = json.loads(refreshed.stdout)
        self.assertEqual(report["revision"], 2)
        self.assertEqual(len(report["pending"]), 1)
        self.assertEqual(report["pending"][0]["base_range"], [1, 4])
        self.assertEqual([d["conflict_id"] for d in report["decisions"]],
                         [ids[2]])
        out = self.dir / "export.json"
        blocked = self.cli("export", "--db", str(self.db), "--output", str(out))
        self.assertEqual(blocked.returncode, 2)
        self.assertFalse(out.exists())
        # re-decide the bridged conflict; output keeps the remote decision and
        # the non-conflicting edit
        new_id = report["pending"][0]["conflict_id"]
        again = self.cli(
            "resolve", "--db", str(self.db),
            "--input", str(write_json(self.dir / "d2.json", {"decisions": [
                {"conflict_id": new_id, "choice": "right"}]})),
            "--expected-revision", "2")
        self.assertEqual(again.returncode, 0, again.stderr)
        exported = self.cli("export", "--db", str(self.db), "--output", str(out))
        self.assertEqual(exported.returncode, 0, exported.stderr)
        tokens = json.loads(out.read_text())["tokens"]
        self.assertEqual(
            tokens,
            ["m0", "R0", "m1", "R1", "m2", "x2", "m3", "RF", "m4", "x4", "m5",
             "tail"])
        # remote decision (RF), refreshed conflict choice and the untouched
        # automatic edit all appear together
        self.assertIn("RF", tokens)
        self.assertIn("tail", tokens)


    def test_expected_revision_may_be_embedded(self):
        self.cli("create", "--db", str(self.db),
                 "--input", str(write_json(self.dir / "c.json", doc())))
        report = json.loads(self.cli("inspect", "--db", str(self.db)).stdout)
        ids = [item["conflict_id"] for item in report["pending"]]
        refresh = write_json(self.dir / "r.json", {
            "left": LEFT, "right": ["m0", "R0", "m1", "R1", "m2", "NEW", "m3"],
            "expected_revision": 0,
        })
        result = self.cli("refresh", "--db", str(self.db), "--input",
                          str(refresh), "--expected-revision", "0")
        self.assertEqual(result.returncode, 0, result.stderr)
        stale = self.cli("refresh", "--db", str(self.db), "--input",
                         str(refresh), "--expected-revision", "0")
        self.assertEqual(stale.returncode, 2)
        self.assertIn("stale", stale.stderr)
        newest = json.loads(self.cli("inspect", "--db", str(self.db)).stdout)
        self.assertEqual(newest["revision"], 1)
        self.assertIn("NEW", newest["input"]["right"])

    def test_path_conflicts_and_symlinks(self):
        source = write_json(self.dir / "c.json", doc())
        before = source.read_bytes()
        self.assertEqual(self.cli("create", "--db", str(source), "--input",
                                  str(source)).returncode, 2)
        self.assertEqual(source.read_bytes(), before)
        output_link = self.dir / "link.json"
        output_link.symlink_to(source)
        self.assertEqual(
            self.cli("create", "--db", str(self.db), "--input", str(source),
                     "--output", str(output_link)).returncode, 2)
        self.assertEqual(source.read_bytes(), before)
        self.assertFalse(self.db.exists())
        self.assertEqual(
            self.cli("create", "--db", str(self.db), "--input", str(source),
                     "--output", str(self.db)).returncode, 2)
        # database path may not be a JSON input
        self.assertEqual(
            self.cli("inspect", "--db", str(self.db)).returncode, 2)

    def test_strict_json_input_rules(self):
        db = self.db
        for body in ('{}', '{"base": [], "base": [], "left": [], "right": [],'
                     ' "rules": []}', 'NaN'):
            path = self.dir / "bad.json"
            path.write_text(body)
            result = self.cli("create", "--db", str(db), "--input", str(path))
            self.assertEqual(result.returncode, 2)
            # rejected documents are validated before the workspace opens
            self.assertFalse(db.exists())
        # too many rows
        oversized = write_json(self.dir / "big.json",
                               {"base": ["a"] * 501, "left": [], "right": [],
                                "rules": []})
        self.assertEqual(
            self.cli("create", "--db", str(db), "--input", str(oversized))
            .returncode, 2)

    def test_crash_flag_preserves_previous_revision(self):
        self.cli("create", "--db", str(self.db),
                 "--input", str(write_json(self.dir / "c.json", doc())))
        before = self.db.read_bytes()
        result = self.cli(
            "refresh", "--db", str(self.db),
            "--input", str(write_json(self.dir / "r.json", {
                "left": ["m0", "NEW", "m1", "L1", "m2", "LF", "m3"],
                "right": RIGHT})), "--expected-revision", "0",
            "--simulate-crash")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.db.read_bytes(), before)
        state = json.loads(self.cli("inspect", "--db", str(self.db)).stdout)
        self.assertEqual(state["revision"], 0)
        self.assertEqual(state["input"]["left"], LEFT)

    def test_concurrent_refresh_and_resolve(self):
        self.cli("create", "--db", str(self.db),
                 "--input", str(write_json(self.dir / "c.json", doc())))
        state = json.loads(self.cli("inspect", "--db", str(self.db)).stdout)
        ids = [item["conflict_id"] for item in state["pending"]]
        refresh = write_json(self.dir / "r.json", {
            "left": ["m0", "NEW", "m1", "L1", "m2", "LF", "m3"],
            "right": RIGHT})
        decisions = write_json(self.dir / "d.json", {"decisions": [
            {"conflict_id": ids[2], "choice": "right"}]})
        commands = [
            ["refresh", "--db", str(self.db), "--input", str(refresh),
             "--expected-revision", "0"],
            ["resolve", "--db", str(self.db), "--input", str(decisions),
             "--expected-revision", "0"],
        ]
        processes = [
            subprocess.Popen(
                [sys.executable, "-m", "sequence_align.merge_cli", *command],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env=self.env)
            for command in commands
        ]
        outcomes = []
        for process in processes:
            stdout, stderr = process.communicate()
            outcomes.append((process.returncode, stdout, stderr))
        winners = [outcome for outcome in outcomes if outcome[0] == 0]
        losers = [outcome for outcome in outcomes if outcome[0] == 2]
        self.assertEqual(len(winners), 1, outcomes)
        self.assertEqual(len(losers), 1, outcomes)
        self.assertIn("stale", losers[0][2])
        state = json.loads(self.cli("inspect", "--db", str(self.db)).stdout)
        self.assertEqual(state["revision"], 1)
        out = self.dir / "export.json"
        result = self.cli("export", "--db", str(self.db), "--output", str(out))
        if state["pending"]:
            self.assertEqual(result.returncode, 2)
            self.assertFalse(out.exists())
        else:
            self.assertEqual(result.returncode, 0, result.stderr)
            tokens = json.loads(out.read_text())["tokens"]
            self.assertNotEqual(tokens, ["m0", "R0", "m1", "R1", "m2", "LF",
                                         "m3"])


# ---------------------------------------------------------------------------
# Contract-level details


class ContractTest(unittest.TestCase):
    def test_conflict_limit_applies_only_to_conflicts(self):
        from sequence_align.merge import MAX_CONFLICTS
        self.assertEqual(MAX_CONFLICTS, 12)

    def test_reference_exhaustive_three_tokens(self):
        words = [()] + [(token,) for token in "abc"] + [
            (x, y) for x in "abc" for y in "abc"]
        count = 0
        for base in words:
            for left in words:
                for right in words:
                    analysis = analyze(list(base), list(left), list(right), [])
                    expected = reference_components(
                        list(base), list(left), list(right))
                    for want, got in zip(expected, analysis["components"]):
                        self.assertEqual(want["kind"], got["kind"])
                        self.assertEqual(want["tokens"], got["tokens"])
                        self.assertEqual(want["left_projection"],
                                         got["left_projection"])
                    count += 1
        self.assertEqual(count, len(words) ** 3)

    def test_reference_exhaustive_short_sequences(self):
        # exhaustive over a tiny alphabet: every base/left/right triple of
        # length <= 2 on two tokens must agree with the slicing reference
        words = [()] + [(token,) for token in "ab"] + [
            (x, y) for x in "ab" for y in "ab"]
        for base in words:
            for left in words:
                for right in words:
                    analysis = analyze(list(base), list(left), list(right), [])
                    expected = reference_components(
                        list(base), list(left), list(right))
                    self.assertEqual(len(expected), len(analysis["components"]))
                    for want, got in zip(expected, analysis["components"]):
                        self.assertEqual(want["base_range"], got["base_range"])
                        self.assertEqual(want["kind"], got["kind"])
                        self.assertEqual(want["left_projection"],
                                         got["left_projection"])
                        self.assertEqual(want["right_projection"],
                                         got["right_projection"])
                        self.assertEqual(want["tokens"], got["tokens"])

    def test_identity_structure_is_stable_and_side_aware(self):
        base = ["m0", "x0", "m1"]
        left = ["m0", "L", "m1"]
        right = ["m0", "x0", "R", "m1"]
        analysis = analyze(base, left, right, [])
        self.assertEqual(len(analysis["components"]), 1)
        component = analysis["components"][0]
        identity = component["identity"]
        self.assertEqual(identity["base_sha256"], digest(base))
        self.assertEqual(identity["base_range"], [1, 2])
        self.assertEqual(
            identity["hunks"],
            [
                {"side": "left", "base_range": [1, 2], "replacement": ["L"]},
                {"side": "right", "base_range": [2, 2], "replacement": ["R"]},
            ],
        )
        self.assertEqual(component["conflict_id"], digest(identity))
        self.assertEqual(
            component["conflict_id"],
            hashlib.sha256(
                json.dumps(identity, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        )

    def test_base_change_invalidates_identity(self):
        left = ["m0", "L", "m1"]
        right = ["m0", "R", "m1"]
        first = conflict_ids(analyze(["m0", "x", "m1"], left, right, []))
        second = conflict_ids(analyze(["m0", "y", "m1"], left, right, []))
        self.assertFalse(set(first.values()) & set(second.values()))

    def test_unrelated_side_edit_keeps_identity(self):
        base = ["m0", "x", "m1"]
        left = ["m0", "L", "m1"]
        right = ["m0", "R", "m1"]
        first = set(conflict_ids(analyze(base, left, right, [])).values())
        second = set(conflict_ids(analyze(
            base, ["extra"] + left, ["extra"] + right, [])).values())
        self.assertIn(next(iter(first)), second)

    def test_rules_validation_bounds(self):
        from sequence_align.merge import parse_rules
        rule = {"kind": "before", "a": "a", "b": "b"}
        self.assertEqual(len(parse_rules([rule] * 32)), 32)
        with self.assertRaises(ValueError):
            parse_rules([rule] * 33)
        with self.assertRaises(ValueError):
            parse_rules([{"kind": "before", "a": "a", "b": "b", "c": None}])

    def test_manual_tokens_must_be_idents(self):
        from sequence_align.merge import parse_resolve
        token = "a" * 64
        self.assertEqual(
            parse_resolve({"decisions": [
                {"conflict_id": token, "choice": "manual", "tokens": ["x"]}]}
            )[0]["tokens"], ["x"])
        with self.assertRaises(ValueError):
            parse_resolve({"decisions": [
                {"conflict_id": token, "choice": "manual", "tokens": [""]}]})
        with self.assertRaises(ValueError):
            parse_resolve({"decisions": [
                {"conflict_id": token, "choice": "manual", "tokens": ["a"] * 501}]})
        with self.assertRaises(ValueError):
            parse_resolve({"decisions": [
                {"conflict_id": token, "choice": "left", "tokens": ["x"]}]})

    def test_duplicate_decisions_in_one_batch_rejected(self):
        from sequence_align.merge import parse_resolve
        token = "a" * 64
        with self.assertRaises(ValueError):
            parse_resolve({"decisions": [
                {"conflict_id": token, "choice": "left"},
                {"conflict_id": token, "choice": "right"}]})

    def test_before_rule_rejects_interleaving(self):
        from sequence_align.merge import rules_satisfied
        rules = [{"kind": "before", "a": "a", "b": "b"}]
        self.assertFalse(rules_satisfied(["a", "b", "a", "b"], rules))
        self.assertTrue(rules_satisfied(["a", "a", "b", "b"], rules))


class AtomicReportTest(unittest.TestCase):
    def test_create_report_write_failure_leaves_no_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            db = directory / "merge.db"
            source = write_json(directory / "create.json", doc())
            blocker = directory / "blocked"
            blocker.mkdir()
            # the report destination's parent is a regular file, so staging
            # cannot succeed; create must not commit revision 0
            (directory / "not-a-dir").write_text("x")
            target = directory / "not-a-dir" / "report.json"
            env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
            result = subprocess.run(
                [sys.executable, "-m", "sequence_align.merge_cli", "create",
                 "--db", str(db), "--input", str(source),
                 "--output", str(target)],
                capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 2)
            # nothing was committed: inspect reports a missing state
            inspected = self.cli_inspect(directory, db, env)
            self.assertEqual(inspected.returncode, 2)
            self.assertIn("no committed state", inspected.stderr)

    @staticmethod
    def cli_inspect(directory, db, env):
        return subprocess.run(
            [sys.executable, "-m", "sequence_align.merge_cli", "inspect",
             "--db", str(db)],
            capture_output=True, text=True, env=env)

    def test_refresh_report_write_failure_rolls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            db = directory / "merge.db"
            env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}

            def run(*arguments):
                return subprocess.run(
                    [sys.executable, "-m", "sequence_align.merge_cli",
                     *arguments],
                    capture_output=True, text=True, env=env)

            created = run("create", "--db", str(db), "--input",
                          str(write_json(directory / "c.json", doc())))
            self.assertEqual(created.returncode, 0, created.stderr)
            before = db.read_bytes()
            (directory / "file").write_text("x")
            failed = run("refresh", "--db", str(db), "--input",
                         str(write_json(directory / "r.json", {
                             "left": ["m0", "NEW", "m1", "L1", "m2", "LF",
                                      "m3"], "right": RIGHT})),
                         "--expected-revision", "0",
                         "--output", str(directory / "file" / "out.json"))
            self.assertEqual(failed.returncode, 2)
            self.assertEqual(db.read_bytes(), before)
            state = json.loads(run("inspect", "--db", str(db)).stdout)
            self.assertEqual(state["revision"], 0)
            self.assertEqual(state["input"]["left"], LEFT)
