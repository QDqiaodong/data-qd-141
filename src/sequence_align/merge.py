"""Three-way sequence merge analysis built on the deterministic aligner.

The merge analysis never compares token *sets*: ``base`` is aligned to
``left`` and to ``right`` with the same distance, tie-breaking and cursor
semantics as :mod:`sequence_align.alignment`.  Every maximal run of
non-``match`` operations becomes one hunk.  A hunk records the half-open base
interval ``[l, r)`` it consumes and the replacement tokens it emits.  A pure
insertion consumes ``[g, g)``.

Only hunks from *different* sides are joined:

* two non-empty intervals intersect;
* a pure insertion point ``g`` lies in a non-empty interval's closed range
  ``[l, r]`` (so an insertion at a deletion endpoint still interacts);
* two pure insertions share the same ``g``.

Each connected component is rendered independently on the base slice covered
by its members.  A one-sided component applies automatically.  A two-sided
component applies automatically when both projections are identical, and is a
conflict otherwise.  Base tokens outside every component keep their original
order; equal tokens at different positions are never collapsed.

Conflict identity deliberately contains only the fixed base digest, the
component's base range and the ordered member hunks.  It does not contain the
side target indexes, a component ordinal, or a whole left/right digest, so an
unrelated modification elsewhere in a refreshed side leaves the identity (and
therefore a saved decision) intact.
"""
from __future__ import annotations

import hashlib
import json
from itertools import product

from .alignment import align
from .common import ident, rows

MAX_TOKENS = 500
MAX_RULES = 32
MAX_CONFLICTS = 12
MAX_MANUAL = 500
MAX_DECISIONS = 64

_SIDE_ORDER = {"left": 0, "right": 1}


# ---------------------------------------------------------------------------
# Validation and canonical encoding


def canonical_json(value) -> str:
    """Deterministic JSON: sorted keys, no whitespace, UTF-8, raw non-ASCII."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def token_list(value, name="tokens", limit=MAX_TOKENS):
    """Validate an identifier array; empty and duplicates are legal."""
    return [ident(item) for item in rows(value, limit)]


def parse_rules(value):
    result = []
    for index, item in enumerate(rows(value, MAX_RULES)):
        if not isinstance(item, dict) or set(item) != {"kind", "a", "b"}:
            raise ValueError("rule %d must contain exactly kind/a/b" % index)
        kind = item["kind"]
        if kind not in ("requires", "before"):
            raise ValueError("rule %d kind must be requires or before" % index)
        result.append({"kind": kind, "a": ident(item["a"]),
                       "b": ident(item["b"])})
    return result


def parse_create(data):
    """Validate the exact create document {base, left, right, rules}."""
    if not isinstance(data, dict):
        raise ValueError("create input must be a JSON object")
    if set(data) != {"base", "left", "right", "rules"}:
        raise ValueError("create input must contain exactly base/left/right/rules")
    return {
        "base": token_list(data["base"], "base"),
        "left": token_list(data["left"], "left"),
        "right": token_list(data["right"], "right"),
        "rules": parse_rules(data["rules"]),
    }


def parse_refresh(data):
    """Validate the exact refresh document {left, right}."""
    if not isinstance(data, dict):
        raise ValueError("refresh input must be a JSON object")
    if set(data) != {"left", "right"}:
        raise ValueError("refresh input must contain exactly left/right")
    return {
        "left": token_list(data["left"], "left"),
        "right": token_list(data["right"], "right"),
    }


def _valid_conflict_id(value):
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(c in "0123456789abcdef" for c in value)


def parse_resolve(data):
    """Validate a batch of new/replacement decisions.

    Canonical shape: ``{"decisions": [...]}``.  A bare array is also accepted
    for convenience.  Each decision names a conflict and chooses ``left``,
    ``right`` or a manual token array.
    """
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and set(data) == {"decisions"}:
        items = data["decisions"]
    else:
        raise ValueError("resolve input must contain exactly decisions")
    items = rows(items, MAX_DECISIONS)
    decisions, seen = [], set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError("decision %d must be an object" % index)
        extra = set(item) - {"conflict_id", "choice", "tokens"}
        if extra:
            raise ValueError("decision %d has undefined fields" % index)
        if "conflict_id" not in item or "choice" not in item:
            raise ValueError("decision %d requires conflict_id and choice" % index)
        conflict_id = item["conflict_id"]
        if not _valid_conflict_id(conflict_id):
            raise ValueError("decision %d has an invalid conflict_id" % index)
        if conflict_id in seen:
            raise ValueError("duplicate decision for conflict " + conflict_id)
        seen.add(conflict_id)
        choice = item["choice"]
        if choice not in ("left", "right", "manual"):
            raise ValueError("decision %d choice must be left, right or manual" % index)
        if choice == "manual":
            if "tokens" not in item:
                raise ValueError("manual decision requires a tokens array")
            tokens = token_list(item["tokens"], "manual tokens", MAX_MANUAL)
        else:
            if item.get("tokens") is not None:
                raise ValueError("tokens are only allowed for manual decisions")
            tokens = None
        decisions.append(
            {"conflict_id": conflict_id, "choice": choice, "tokens": tokens}
        )
    return decisions


# ---------------------------------------------------------------------------
# Hunks and components


def _side_hunks(base, target, side):
    operations = align(list(base), list(target))["operations"]
    hunks, index = [], 0
    while index < len(operations):
        if operations[index]["op"] == "match":
            index += 1
            continue
        start = operations[index]["before_index"]
        end = start
        replacement = []
        cursor = index
        while cursor < len(operations) and operations[cursor]["op"] != "match":
            operation = operations[cursor]
            if operation["source"] is not None:
                end = operation["before_index"] + 1
            if operation["target"] is not None:
                replacement.append(operation["target"])
            cursor += 1
        hunks.append(
            {
                "side": side,
                "base_range": [start, end],
                "replacement": replacement,
            }
        )
        index = cursor
    return hunks


def _interacts(first, second):
    if first["side"] == second["side"]:
        return False
    l1, r1 = first["base_range"]
    l2, r2 = second["base_range"]
    pure1, pure2 = l1 == r1, l2 == r2
    if not pure1 and not pure2:
        return max(l1, l2) < min(r1, r2)
    if pure1 and pure2:
        return l1 == l2
    if pure1:
        return l2 <= l1 <= r2
    return l1 <= l2 <= r1


def _hunk_key(hunk):
    return (
        _SIDE_ORDER[hunk["side"]],
        hunk["base_range"][0],
        hunk["base_range"][1],
        tuple(hunk["replacement"]),
    )


def _components(hunks):
    parent = list(range(len(hunks)))

    def find(item):
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for first in range(len(hunks)):
        for second in range(first + 1, len(hunks)):
            if _interacts(hunks[first], hunks[second]):
                union(first, second)

    grouped = {}
    for index, hunk in enumerate(hunks):
        grouped.setdefault(find(index), []).append(hunk)
    return list(grouped.values())


def _project(base, low, high, side_hunks):
    out, cursor = [], low
    for hunk in sorted(side_hunks, key=_hunk_key):
        start, end = hunk["base_range"]
        if start < cursor:
            raise AssertionError("overlapping hunks on one side")
        out.extend(base[cursor:start])
        out.extend(hunk["replacement"])
        cursor = end
    out.extend(base[cursor:high])
    return out


def _conflict_identity(base_sha256, low, high, members):
    payload = {
        "base_sha256": base_sha256,
        "base_range": [low, high],
        "hunks": [
            {
                "side": hunk["side"],
                "base_range": list(hunk["base_range"]),
                "replacement": list(hunk["replacement"]),
            }
            for hunk in sorted(members, key=_hunk_key)
        ],
    }
    return digest(payload), payload


def analyze(base, left, right, rules):
    """Return the complete component view for one base/left/right triple."""
    base = list(base)
    left = list(left)
    right = list(right)
    base_sha256 = digest(base)
    hunks = _side_hunks(base, left, "left")
    hunks += _side_hunks(base, right, "right")

    components = []
    for group in _components(hunks):
        members = sorted(group, key=_hunk_key)
        low = min(hunk["base_range"][0] for hunk in members)
        high = max(hunk["base_range"][1] for hunk in members)
        left_members = [h for h in members if h["side"] == "left"]
        right_members = [h for h in members if h["side"] == "right"]
        left_projection = (
            _project(base, low, high, left_members) if left_members else None
        )
        right_projection = (
            _project(base, low, high, right_members) if right_members else None
        )
        sides = sorted({h["side"] for h in members}, key=_SIDE_ORDER.get)

        if not left_members:
            kind, tokens = "automatic", right_projection
        elif not right_members:
            kind, tokens = "automatic", left_projection
        elif left_projection == right_projection:
            kind, tokens = "automatic", left_projection
        else:
            kind, tokens = "conflict", None

        component = {
            "kind": kind,
            "sides": sides,
            "base_range": [low, high],
            "hunks": members,
            "left_projection": left_projection,
            "right_projection": right_projection,
            "tokens": tokens,
        }
        if kind == "conflict":
            conflict_id, identity = _conflict_identity(
                base_sha256, low, high, members
            )
            component["conflict_id"] = conflict_id
            component["identity"] = identity
        components.append(component)

    components.sort(
        key=lambda c: (
            c["base_range"][0],
            c["base_range"][1],
            0 if c["kind"] == "automatic" else 1,
            tuple(c["sides"]),
        )
    )
    for index, component in enumerate(components):
        component["index"] = index
    return {
        "base": base,
        "left": left,
        "right": right,
        "rules": rules,
        "base_sha256": base_sha256,
        "left_sha256": digest(left),
        "right_sha256": digest(right),
        "rules_sha256": digest(rules),
        "components": components,
    }


def conflict_map(analysis):
    return {
        component["conflict_id"]: component
        for component in analysis["components"]
        if component["kind"] == "conflict"
    }


def automatic_count(analysis):
    return sum(c["kind"] == "automatic" for c in analysis["components"])


# ---------------------------------------------------------------------------
# Rules and complete merged lists


def rules_satisfied(tokens, rules):
    first, last = {}, {}
    for index, token in enumerate(tokens):
        if token not in first:
            first[token] = index
        last[token] = index
    for rule in rules:
        a, b = rule["a"], rule["b"]
        if rule["kind"] == "requires":
            if a in first and b not in first:
                return False
        else:  # before: every a occurrence precedes every b occurrence
            if a in first and b in first and not last[a] < first[b]:
                return False
    return True


def _chosen_tokens(component, decision):
    if decision["choice"] == "left":
        return component["left_projection"]
    if decision["choice"] == "right":
        return component["right_projection"]
    return decision["tokens"]


def build_tokens(analysis, decisions):
    """Apply automatic components and fixed conflict decisions in base order.

    ``decisions`` must already contain a valid decision for every conflict.
    The returned ``sources`` trace which base interval produced each output
    segment and which member hunks were involved.
    """
    tokens, sources, cursor = [], [], 0
    for component in analysis["components"]:
        low, high = component["base_range"]
        if low < cursor:
            raise AssertionError("overlapping components")
        if low > cursor:
            start = len(tokens)
            tokens.extend(analysis["base"][cursor:low])
            sources.append(
                {
                    "kind": "base",
                    "base_range": [cursor, low],
                    "output_range": [start, len(tokens)],
                    "tokens": list(analysis["base"][cursor:low]),
                }
            )
        start = len(tokens)
        if component["kind"] == "automatic":
            chosen = component["tokens"]
            choice = "automatic"
        else:
            decision = decisions[component["conflict_id"]]
            chosen = _chosen_tokens(component, decision)
            choice = decision["choice"]
        tokens.extend(chosen)
        sources.append(
            {
                "kind": component["kind"],
                "base_range": [low, high],
                "output_range": [start, len(tokens)],
                "tokens": list(chosen),
                "hunks": component["hunks"],
                "choice": choice,
                "conflict_id": component.get("conflict_id"),
            }
        )
        cursor = high
    if cursor < len(analysis["base"]):
        start = len(tokens)
        tokens.extend(analysis["base"][cursor:])
        sources.append(
            {
                "kind": "base",
                "base_range": [cursor, len(analysis["base"])],
                "output_range": [start, len(tokens)],
                "tokens": list(analysis["base"][cursor:]),
            }
        )
    return tokens, sources


def _fixed_decisions(analysis, decisions):
    conflicts = conflict_map(analysis)
    return {
        conflict_id: decision
        for conflict_id, decision in decisions.items()
        if conflict_id in conflicts
    }


def completion_search(analysis, decisions):
    """Enumerate left/right completions of every still-pending conflict.

    Returns ``(feasible, support)`` where ``support[conflict_id]`` has the
    booleans ``left`` and ``right``: does some complete merged list satisfying
    every rule choose that projection?  Manual decisions and automatic
    components are fixed; the search never invents manual tokens for another
    conflict.
    """
    conflicts = conflict_map(analysis)
    fixed = _fixed_decisions(analysis, decisions)
    pending = [
        component
        for component in analysis["components"]
        if component["kind"] == "conflict"
        and component["conflict_id"] not in fixed
    ]
    support = {
        component["conflict_id"]: {"left": False, "right": False}
        for component in pending
    }
    feasible = False
    ids = [component["conflict_id"] for component in pending]
    for choices in product(("left", "right"), repeat=len(pending)):
        trial = dict(fixed)
        for conflict_id, choice in zip(ids, choices):
            trial[conflict_id] = {"choice": choice, "tokens": None}
        tokens, _ = build_tokens(analysis, trial)
        if not rules_satisfied(tokens, analysis["rules"]):
            continue
        feasible = True
        for conflict_id, choice in zip(ids, choices):
            support[conflict_id][choice] = True
    return feasible, support


def export_ready(analysis, decisions):
    """Return ``(ready, tokens, sources)`` for a fully decided analysis."""
    conflicts = conflict_map(analysis)
    if any(component["conflict_id"] not in decisions
           for component in conflicts.values()):
        return False, None, None
    tokens, sources = build_tokens(analysis, decisions)
    if not rules_satisfied(tokens, analysis["rules"]):
        return False, tokens, sources
    return True, tokens, sources
