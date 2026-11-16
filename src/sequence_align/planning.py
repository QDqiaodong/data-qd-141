"""Field-version migration planning built on deterministic sequence alignment.

The planner reuses :func:`sequence_align.alignment.align`; the minimum edit
script only *names and orders* candidates:

* an ``add`` candidate is strictly a field occurring in ``after`` but not in
  ``before``;
* a ``drop`` candidate is strictly the symmetric case;
* a field shared by both snapshots only ever moves and never yields a
  candidate;
* a ``replace`` operation is only a display grouping of one add and one drop;
  both candidate identities are unchanged.

Candidate order and stable numbering: by the index of the first alignment
operation whose source or target is the field, then adds before drops, then
field name.  Numbers start at 1 in that order.

A plan is a contiguous, non-idle sequence of stages ``1..H`` with ``1 <= H <=
8``.  Every stage changes at most two fields and always runs in this order:

    add fields, switch consumers, drop fields

so a field dropped in a stage is still readable by consumers switching in that
stage, and an added field is already readable by them.  Consumers switch
exactly once, inside their window.  Before its switch a consumer reads
``reads_before``; afterwards it reads ``reads_after``.  The final field set is
exactly ``after``; the result also keeps the target order of ``after``.
"""
from functools import lru_cache

from .alignment import align
from .common import ident, require

MAX_FIELDS = 8
MAX_CONSUMERS = 6
MAX_STAGES = 8
MAX_CHANGES_PER_STAGE = 2

STATUS_OK = "satisfiable"
STATUS_UNSAT = "unsatisfiable"


# ---------------------------------------------------------------------------
# Validation


def _fields(value, name, allow_empty=False):
    if not isinstance(value, list):
        raise ValueError("expected array for " + name)
    if len(value) > MAX_FIELDS:
        raise ValueError(name + " exceeds 8 fields")
    if not value and not allow_empty:
        raise ValueError(name + " must not be empty")
    result, seen = [], set()
    for item in value:
        ident(item)
        if item in seen:
            raise ValueError("duplicate field in " + name + ": " + item)
        seen.add(item)
        result.append(item)
    return result


def _window(value, name):
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError(name + " must be a two-element [lo, hi] window")
    lo, hi = value
    for bound in (lo, hi):
        if type(bound) is not int:
            raise ValueError(name + " window bounds must be integers")
    if not 1 <= lo <= hi <= MAX_STAGES:
        raise ValueError(name + " must satisfy 1 <= lo <= hi <= 8")
    return (lo, hi)


def parse(data):
    """Validate and normalise one planning document.

    Structural errors raise ``ValueError`` so the CLI keeps the legacy
    exit-code contract (exit 2, previous report untouched).  Repeated fields in
    ``before``/``after`` are rejected only by this new entry point; the legacy
    single-alignment entry point keeps accepting repeated tokens.
    """
    require(data, "before after consumers")
    before = _fields(data["before"], "before")
    after = _fields(data["after"], "after")
    raw = data["consumers"]
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_CONSUMERS:
        raise ValueError("consumers must be an array of 1 to 6 objects")

    before_set, after_set = set(before), set(after)
    consumers, ids = [], set()
    for item in raw:
        require(item, "id reads_before reads_after window")
        cid = ident(item["id"])
        if cid in ids:
            raise ValueError("duplicate consumer id: " + cid)
        ids.add(cid)
        reads_before = _fields(item["reads_before"], "reads_before", allow_empty=True)
        reads_after = _fields(item["reads_after"], "reads_after", allow_empty=True)
        for field in reads_before:
            if field not in before_set:
                raise ValueError("reads_before references %r outside before" % field)
        for field in reads_after:
            if field not in after_set:
                raise ValueError("reads_after references %r outside after" % field)
        consumers.append(
            {
                "id": cid,
                "reads_before": tuple(reads_before),
                "reads_after": tuple(reads_after),
                "window": _window(item["window"], cid + ".window"),
            }
        )

    return {
        "before": tuple(before),
        "after": tuple(after),
        "consumers": tuple(consumers),
    }


# ---------------------------------------------------------------------------
# Candidates


def candidates(before, after):
    """Return stably numbered add/drop candidates for one field diff."""
    before_set, after_set = set(before), set(after)
    adds = [field for field in after if field not in before_set]
    drops = [field for field in before if field not in after_set]
    add_set, drop_set = set(adds), set(drops)

    operations = align(list(before), list(after))["operations"]
    first = {}
    for index, op in enumerate(operations):
        for field in (op["source"], op["target"]):
            if field is None or not (field in add_set or field in drop_set):
                continue
            if field not in first or index < first[field]:
                first[field] = index
    for field in adds + drops:
        first.setdefault(field, len(operations))

    ordered = [("add", field, first[field], 0) for field in adds]
    ordered += [("drop", field, first[field], 1) for field in drops]
    ordered.sort(key=lambda entry: (entry[2], entry[3], entry[1]))

    replace_of = {}
    for op in operations:
        if op["op"] != "replace":
            continue
        source, target = op["source"], op["target"]
        if source in drop_set and target in add_set:
            replace_of[target] = source

    result, seen = [], set()
    for kind, field, _, _ in ordered:
        if (kind, field) in seen:
            continue
        seen.add((kind, field))
        result.append(
            {
                "number": len(result) + 1,
                "kind": kind,
                "field": field,
                "replace_with": replace_of.get(field) if kind == "add" else None,
            }
        )
    return result


# ---------------------------------------------------------------------------
# Model
#
# Items with identical scheduling behaviour are interchangeable.  Grouping them
# (adds by reader set, drops by reader set, consumers by window and read groups)
# keeps the exact search small whenever symmetric items exist.


class _Model:
    def __init__(self, problem):
        self.before = list(problem["before"])
        self.after = list(problem["after"])
        self.candidate_list = candidates(self.before, self.after)
        self.adds = [c["field"] for c in self.candidate_list if c["kind"] == "add"]
        self.drops = [c["field"] for c in self.candidate_list if c["kind"] == "drop"]
        self.add_number = {
            c["field"]: c["number"] for c in self.candidate_list if c["kind"] == "add"
        }
        self.drop_number = {
            c["field"]: c["number"] for c in self.candidate_list if c["kind"] == "drop"
        }
        self.consumers = list(problem["consumers"])
        self.ids = [c["id"] for c in self.consumers]

        add_readers, drop_readers = {}, {}
        for index, consumer in enumerate(self.consumers):
            for field in consumer["reads_after"]:
                add_readers.setdefault(field, set()).add(index)
            for field in consumer["reads_before"]:
                drop_readers.setdefault(field, set()).add(index)

        add_groups = {}
        for field in self.adds:
            readers = frozenset(add_readers.get(field, ()))
            add_groups.setdefault(readers, []).append(field)
        self.add_groups = []
        self.field_add_group = {}
        for readers, fields in sorted(add_groups.items(), key=lambda e: sorted(e[0])):
            fields.sort(key=lambda f: self.add_number[f])
            self.add_groups.append(
                {
                    "fields": tuple(fields),
                    "numbers": tuple(self.add_number[f] for f in fields),
                    "readers": readers,
                    "deadline": min(
                        (self.consumers[c]["window"][1] for c in readers),
                        default=MAX_STAGES,
                    ),
                }
            )
        # Deadline is a function of the reader set, so ordering by reader set is
        # stable but groups need resorting by deadline for reporting order.
        self.add_groups.sort(
            key=lambda g: (
                g["deadline"],
                g["numbers"],
            )
        )
        for group, entry in enumerate(self.add_groups):
            for field in entry["fields"]:
                self.field_add_group[field] = group

        drop_groups = {}
        for field in self.drops:
            readers = frozenset(drop_readers.get(field, ()))
            drop_groups.setdefault(readers, []).append(field)
        self.drop_groups = []
        self.field_drop_group = {}
        for readers, fields in drop_groups.items():
            fields.sort(key=lambda f: self.drop_number[f])
            self.drop_groups.append(
                {
                    "fields": tuple(fields),
                    "numbers": tuple(self.drop_number[f] for f in fields),
                    "readers": readers,
                    "release": max(
                        (self.consumers[c]["window"][0] for c in readers),
                        default=1,
                    ),
                }
            )
        self.drop_groups.sort(key=lambda g: (-g["release"], g["numbers"]))
        for group, entry in enumerate(self.drop_groups):
            for field in entry["fields"]:
                self.field_drop_group[field] = group

        consumer_groups = {}
        for index, consumer in enumerate(self.consumers):
            lo, hi = consumer["window"]
            signature = (
                lo,
                hi,
                frozenset(
                    self.field_add_group[f]
                    for f in consumer["reads_after"]
                    if f in self.field_add_group
                ),
                frozenset(
                    self.field_drop_group[f]
                    for f in consumer["reads_before"]
                    if f in self.field_drop_group
                ),
            )
            consumer_groups.setdefault(signature, []).append(index)
        self.consumer_groups = []
        for signature, members in sorted(
            consumer_groups.items(),
            key=lambda entry: (entry[0][0], entry[0][1],
                               sorted(entry[0][2]), sorted(entry[0][3])),
        ):
            lo, hi, add_groups_read, drop_groups_read = signature
            members.sort(key=lambda i: self.ids[i])
            self.consumer_groups.append(
                {
                    "ids": tuple(self.ids[i] for i in members),
                    "lo": lo,
                    "hi": hi,
                    "add_groups": tuple(sorted(add_groups_read)),
                    "drop_groups": tuple(sorted(drop_groups_read)),
                }
            )

        self.add_sizes = tuple(len(g["fields"]) for g in self.add_groups)
        self.drop_sizes = tuple(len(g["fields"]) for g in self.drop_groups)
        self.consumer_sizes = tuple(len(g["ids"]) for g in self.consumer_groups)
        self.add_deadlines = tuple(g["deadline"] for g in self.add_groups)
        self.drop_releases = tuple(g["release"] for g in self.drop_groups)
        self.add_numbers = tuple(g["numbers"] for g in self.add_groups)
        self.drop_numbers = tuple(g["numbers"] for g in self.drop_groups)
        self.consumer_ids = tuple(g["ids"] for g in self.consumer_groups)
        # Reader consumer indices -> consumer group index.
        self.consumer_group_of = {}
        for group, entry in enumerate(self.consumer_groups):
            for cid in entry["ids"]:
                self.consumer_group_of[cid] = group
        self.drop_group_readers = tuple(g["readers"] for g in self.drop_groups)

    # -- reporting ----------------------------------------------------------
    def fields_at(self, added, dropped):
        present = []
        for field in self.after:
            add_group = self.field_add_group.get(field)
            if add_group is not None:
                count = added[add_group]
                if field in self.add_groups[add_group]["fields"][:count]:
                    present.append(field)
                continue
            drop_group = self.field_drop_group.get(field)
            if drop_group is not None:
                kept = self.drop_groups[drop_group]["fields"][dropped[drop_group]:]
                if field in kept:
                    present.append(field)
                continue
            present.append(field)
        for field in self.before:
            if field in self.after:
                continue
            drop_group = self.field_drop_group[field]
            if field in self.drop_groups[drop_group]["fields"][dropped[drop_group]:]:
                present.append(field)
        return present


# ---------------------------------------------------------------------------
# Bounded per-group increments


def _increments(sizes, counts, allowed, maximum):
    """Yield per-group increments whose total is at most ``maximum``."""
    groups = len(sizes)

    def walk(group, remaining, prefix):
        if group == groups:
            yield tuple(prefix)
            return
        room = sizes[group] - counts[group]
        top = min(room, remaining) if allowed(group) else 0
        for value in range(top, -1, -1):
            prefix.append(value)
            yield from walk(group + 1, remaining - value, prefix)
            prefix.pop()

    yield from walk(0, maximum, [])


def _product(ranges):
    result = [()]
    for values in ranges:
        result = [prefix + (value,) for prefix in result for value in values]
    return result


def _lowered(counts, increments):
    return tuple(a + b for a, b in zip(counts, increments))


class _Planner:
    def __init__(self, model):
        self.m = model
        self._memo = lru_cache(maxsize=None)(self._min_remaining)

    # -- pruning ------------------------------------------------------------
    def _bounds_ok(self, stage, added, dropped, switched):
        m = self.m
        horizon = MAX_STAGES - stage + 1
        remaining_changes = 0
        for group, size in enumerate(m.add_sizes):
            left = size - added[group]
            if left and m.add_deadlines[group] < stage:
                return False
            remaining_changes += left
        for group, size in enumerate(m.drop_sizes):
            remaining_changes += size - dropped[group]
        if remaining_changes > MAX_CHANGES_PER_STAGE * horizon:
            return False
        for limit in range(stage, MAX_STAGES + 1):
            pending = 0
            for group, size in enumerate(m.add_sizes):
                left = size - added[group]
                if left and m.add_deadlines[group] <= limit:
                    pending += left
            if pending > MAX_CHANGES_PER_STAGE * (limit - stage + 1):
                return False
        for boundary in range(stage, MAX_STAGES + 1):
            pending = 0
            for group, size in enumerate(m.drop_sizes):
                left = size - dropped[group]
                if left and m.drop_releases[group] >= boundary:
                    pending += left
            if pending > MAX_CHANGES_PER_STAGE * (MAX_STAGES - boundary + 1):
                return False
        for group, size in enumerate(m.consumer_sizes):
            left = size - switched[group]
            if left and m.consumer_groups[group]["hi"] < stage:
                return False
        return True

    # -- action enumeration -------------------------------------------------
    def _actions(self, stage, added, dropped, switched):
        """Yield legal (add_inc, drop_inc, switch_inc) stage actions."""
        m = self.m
        add_ok = lambda group: m.add_deadlines[group] >= stage
        drop_ok = lambda group: m.drop_releases[group] <= stage
        for add_inc in _increments(m.add_sizes, added, add_ok, MAX_CHANGES_PER_STAGE):
            added_after = tuple(a + b for a, b in zip(added, add_inc))
            # Adds still pending after this stage must not be past their deadline.
            if any(
                added_after[group] < m.add_sizes[group]
                and m.add_deadlines[group] <= stage
                for group in range(len(m.add_sizes))
            ):
                continue
            ready = tuple(
                added_after[group] == m.add_sizes[group]
                for group in range(len(m.add_sizes))
            )
            budget = MAX_CHANGES_PER_STAGE - sum(add_inc)
            for drop_inc in _increments(m.drop_sizes, dropped, drop_ok, budget):
                ranges = []
                dead = False
                for group, consumer in enumerate(m.consumer_groups):
                    left = m.consumer_sizes[group] - switched[group]
                    if not left:
                        ranges.append((0,))
                        continue
                    if consumer["hi"] < stage:
                        dead = True
                        break
                    eligible = (
                        consumer["lo"] <= stage <= consumer["hi"]
                        and all(ready[g] for g in consumer["add_groups"])
                    )
                    if consumer["hi"] == stage:
                        if not eligible:
                            dead = True
                            break
                        ranges.append((left,))
                    elif eligible:
                        ranges.append(tuple(range(left + 1)))
                    else:
                        ranges.append((0,))
                if dead:
                    continue
                for switch_inc in _product(ranges):
                    if not any(add_inc) and not any(drop_inc) and not any(switch_inc):
                        continue
                    switched_after = tuple(
                        a + b for a, b in zip(switched, switch_inc)
                    )
                    if self._drops_legal(drop_inc, switched_after):
                        yield add_inc, drop_inc, switch_inc

    def _drops_legal(self, drop_inc, switched_after):
        m = self.m
        for group, increment in enumerate(drop_inc):
            if not increment:
                continue
            for reader in m.drop_group_readers[group]:
                reader_group = m.consumer_group_of[m.ids[reader]]
                if switched_after[reader_group] < m.consumer_sizes[reader_group]:
                    return False
        return True

    # -- exact minimum number of remaining stages ---------------------------
    def _min_remaining(self, stage, added, dropped, switched):
        m = self.m
        if (
            added == m.add_sizes
            and dropped == m.drop_sizes
            and switched == m.consumer_sizes
        ):
            return 0
        if stage > MAX_STAGES:
            return None
        if not self._bounds_ok(stage, added, dropped, switched):
            return None
        remaining_changes = 0
        for group, size in enumerate(m.add_sizes):
            remaining_changes += size - added[group]
        for group, size in enumerate(m.drop_sizes):
            remaining_changes += size - dropped[group]
        lower = max(1, remaining_changes // MAX_CHANGES_PER_STAGE)
        best = None
        for add_inc, drop_inc, switch_inc in self._actions(
            stage, added, dropped, switched
        ):
            forward = (
                stage + 1,
                tuple(a + b for a, b in zip(added, add_inc)),
                tuple(a + b for a, b in zip(dropped, drop_inc)),
                tuple(a + b for a, b in zip(switched, switch_inc)),
            )
            tail = self._memo(*forward)
            if tail is not None and (best is None or tail + 1 < best):
                best = tail + 1
                if best == lower:
                    return best
        return best

    # -- lexicographic reconstruction ---------------------------------------
    def build(self):
        m = self.m
        zeros_add = tuple(0 for _ in m.add_sizes)
        zeros_drop = tuple(0 for _ in m.drop_sizes)
        zeros_switch = tuple(0 for _ in m.consumer_sizes)
        total = self._memo(1, zeros_add, zeros_drop, zeros_switch)
        if total is None:
            return None
        stage = 1
        added, dropped, switched = zeros_add, zeros_drop, zeros_switch
        remaining = total
        plan = []
        while remaining:
            options = []
            for add_inc, drop_inc, switch_inc in self._actions(
                stage, added, dropped, switched
            ):
                forward = (
                    stage + 1,
                    tuple(a + b for a, b in zip(added, add_inc)),
                    tuple(a + b for a, b in zip(dropped, drop_inc)),
                    tuple(a + b for a, b in zip(switched, switch_inc)),
                )
                tail = self._memo(*forward)
                if tail is not None and tail + 1 == remaining:
                    options.append(
                        (
                            self._key(
                                added, dropped, switched,
                                add_inc, drop_inc, switch_inc,
                            ),
                            add_inc,
                            drop_inc,
                            switch_inc,
                        )
                    )
            if not options:  # pragma: no cover - oracle consistency guard
                raise AssertionError("planner desynchronised from its oracle")
            options.sort(key=lambda entry: entry[0])
            _, add_inc, drop_inc, switch_inc = options[0]
            plan.append(
                {
                    "stage": stage,
                    "add": self._entries(
                        add_inc, added, m.add_numbers, m.add_groups
                    ),
                    "switch": self._switch_ids(switch_inc, switched),
                    "drop": self._entries(
                        drop_inc, dropped, m.drop_numbers, m.drop_groups
                    ),
                    "fields": m.fields_at(
                        _lowered(added, add_inc), _lowered(dropped, drop_inc)
                    ),
                }
            )
            added = tuple(a + b for a, b in zip(added, add_inc))
            dropped = tuple(a + b for a, b in zip(dropped, drop_inc))
            switched = tuple(a + b for a, b in zip(switched, switch_inc))
            stage += 1
            remaining -= 1
        return plan

    def _key(self, added, dropped, switched, add_inc, drop_inc, switch_inc):
        """Lexicographic key for one stage.

        The compared sequence per stage is ``(ascending consumer ids, ascending
        candidate stable numbers)``; stages are compared in order.  Numbers are
        the stable ids from :func:`candidates`, independent of execution order.
        """
        m = self.m
        numbers = []
        for group, increment in enumerate(add_inc):
            numbers.extend(
                m.add_numbers[group][added[group]:added[group] + increment]
            )
        for group, increment in enumerate(drop_inc):
            numbers.extend(
                m.drop_numbers[group][dropped[group]:dropped[group] + increment]
            )
        ids = []
        for group, increment in enumerate(switch_inc):
            ids.extend(m.consumer_ids[group][switched[group]:switched[group] + increment])
        return tuple(sorted(ids)), tuple(sorted(numbers))

    def _switch_ids(self, switch_inc, switched):
        ids = []
        for group, increment in enumerate(switch_inc):
            ids.extend(
                self.m.consumer_ids[group][switched[group]:switched[group] + increment]
            )
        return sorted(ids)

    def _entries(self, increment, counts, numbers, groups):
        entries = []
        for group, count in enumerate(increment):
            for offset in range(counts[group], counts[group] + count):
                field = groups[group]["fields"][offset]
                entries.append({"number": numbers[group][offset], "field": field})
        entries.sort(key=lambda entry: entry["number"])
        return entries

# ---------------------------------------------------------------------------
# Public entry points


def plan(data):
    """Return the planning report body for one raw input mapping."""
    model = _Model(parse(data))
    stages = _Planner(model).build()
    if stages is None:
        return {
            "status": STATUS_UNSAT,
            "plan": None,
            "reason": _reason(model),
            "after": list(model.after),
        }
    return {
        "status": STATUS_OK,
        "plan": stages,
        "reason": None,
        "after": list(model.after),
    }


def _reason(model):
    """Describe the blocked candidates and the conditions they must satisfy.

    The planner has exhausted every sequence of at most 8 non-idle stages, so
    for each candidate operation no executable transition remains that could
    satisfy the listed prerequisite, window or capacity conditions.
    """
    blocked_candidates = []
    for candidate in model.candidate_list:
        field = candidate["field"]
        if candidate["kind"] == "add":
            readers = [c["id"] for c in model.consumers if field in c["reads_after"]]
            conditions = {
                "precedence": "the add must run before the first switch reading it",
                "dependent_consumers": readers,
                "target_must_be_absent_before_the_add": True,
                "capacity_per_stage": MAX_CHANGES_PER_STAGE,
            }
        else:
            readers = [c["id"] for c in model.consumers if field in c["reads_before"]]
            conditions = {
                "precedence": "the drop must run after the last switch still reading it",
                "dependent_consumers": readers,
                "target_must_be_present_before_the_drop": True,
                "capacity_per_stage": MAX_CHANGES_PER_STAGE,
            }
        blocked_candidates.append(
            {
                "number": candidate["number"],
                "kind": candidate["kind"],
                "field": field,
                "replace_with": candidate["replace_with"],
                "conditions": conditions,
            }
        )
    blocked_consumers = []
    for consumer in model.consumers:
        lo, hi = consumer["window"]
        blocked_consumers.append(
            {
                "id": consumer["id"],
                "window": [lo, hi],
                "reads_before": list(consumer["reads_before"]),
                "reads_after": list(consumer["reads_after"]),
                "conditions": {
                    "exactly_one_switch_within_window": [lo, hi],
                    "reads_before_must_exist_at_switch": list(consumer["reads_before"]),
                    "reads_after_must_exist_at_switch": list(consumer["reads_after"]),
                },
            }
        )
    return {
        "message": (
            "no schedule exists with 1..8 non-idle stages; every remaining "
            "add/drop/consumer transition violates a prerequisite, window or "
            "per-stage capacity condition listed below"
        ),
        "stage_limit": MAX_STAGES,
        "capacity_per_stage": MAX_CHANGES_PER_STAGE,
        "blocked_candidates": blocked_candidates,
        "blocked_consumers": blocked_consumers,
    }
