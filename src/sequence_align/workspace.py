"""Crash-safe SQLite workbench for reviewing a three-way sequence merge.

All persistent state of a workspace lives in one row of one SQLite table.  A
mutation (``create``, ``refresh``, ``resolve``) runs inside a single
``BEGIN IMMEDIATE`` transaction that re-reads the revision, recomputes the
component view from the stored base/left/right/rules, derives the surviving
decisions and writes revision + inputs + decisions together.  A stale
``expected_revision`` or any later failure rolls the whole transaction back,
so concurrent processes observe either the old state or the new state, never a
refresh carrying decisions inherited from a different component set.

Read-only operations (``inspect``, ``export``) open a separate ``mode=ro``
connection and hold one transaction for the whole read, so a concurrent commit
cannot mix refreshed components with old decisions.  ``export`` never writes
the workspace.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading

from .common import discard, publish
from .merge import (
    MAX_CONFLICTS,
    analyze,
    canonical_json,
    completion_search,
    conflict_map,
    export_ready,
    parse_create,
    parse_refresh,
    parse_resolve,
)

SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS merge_workspace(
    id                 INTEGER PRIMARY KEY CHECK(id = 1),
    revision           INTEGER NOT NULL,
    base_json          TEXT NOT NULL,
    left_json          TEXT NOT NULL,
    right_json         TEXT NOT NULL,
    rules_json         TEXT NOT NULL,
    decisions_json     TEXT NOT NULL,
    create_source_sha256 TEXT,
    updated_source_sha256 TEXT
);
"""


class WorkspaceError(Exception):
    """Base class for refusals that leave the workspace untouched."""


class StaleRevisionError(WorkspaceError):
    """expected_revision does not match the committed revision."""


class WorkspaceExistsError(WorkspaceError):
    """create was called on an already initialised workspace."""


class WorkspaceMissingError(WorkspaceError):
    """The workspace has no committed state yet."""


class UnknownConflictError(WorkspaceError):
    """A resolve batch names a conflict outside the current component view."""


class InfeasibleBatchError(WorkspaceError):
    """A resolve batch would leave no rule-satisfying complete list."""


class PendingConflictError(WorkspaceError):
    """export was requested while at least one conflict is undecided."""


class RulesViolationError(WorkspaceError):
    """export was requested with decisions that violate a data rule."""


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _decisions_payload(decisions):
    return canonical_json(
        {
            conflict_id: {
                "choice": decision["choice"],
                "tokens": decision["tokens"],
            }
            for conflict_id, decision in sorted(decisions.items())
        }
    )


def _parse_stored_decisions(raw):
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError("stored decisions are not an object")
    result = {}
    for conflict_id, decision in value.items():
        if not isinstance(decision, dict):
            raise RuntimeError("stored decision is not an object")
        choice = decision.get("choice")
        tokens = decision.get("tokens")
        if choice not in ("left", "right", "manual"):
            raise RuntimeError("stored decision has an invalid choice")
        if tokens is not None and not isinstance(tokens, list):
            raise RuntimeError("stored decision tokens are not an array")
        result[conflict_id] = {"choice": choice, "tokens": tokens}
    return result


def _read_json(row, column):
    return json.loads(row[column])


class MergeWorkspace:
    """Owns one SQLite file holding a merge review workspace."""

    def __init__(self, path, *, simulate_crash_before_commit=False,
                 read_only=False):
        self.path = os.path.abspath(path)
        self.read_only = read_only
        self._staged = None
        self._lock = threading.RLock()
        self._simulate_crash = simulate_crash_before_commit
        if read_only:
            if not os.path.exists(self.path):
                raise WorkspaceMissingError("workspace has no committed state")
            from urllib.request import pathname2url

            uri = "file:" + pathname2url(self.path) + "?mode=ro"
            self._writer = sqlite3.connect(
                uri, uri=True, isolation_level=None, check_same_thread=False
            )
            self._writer.execute("PRAGMA query_only=ON")
            self._writer.execute("PRAGMA busy_timeout=15000")
            self._writer.row_factory = sqlite3.Row
            if self._writer.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='merge_workspace'"
            ).fetchone() is None:
                self._writer.close()
                raise WorkspaceMissingError("workspace has no committed state")
            return
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._writer = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self._writer.row_factory = sqlite3.Row
        self._writer.execute("PRAGMA busy_timeout=15000")
        self._initialize()

    def _initialize(self):
        with self._lock:
            self._writer.executescript(SCHEMA_SQL)

    def close(self):
        with self._lock:
            self._writer.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------------- helpers
    def _row(self):
        return self._writer.execute(
            "SELECT * FROM merge_workspace WHERE id = 1"
        ).fetchone()

    def _state(self, row):
        return {
            "revision": int(row["revision"]),
            "base": _read_json(row, "base_json"),
            "left": _read_json(row, "left_json"),
            "right": _read_json(row, "right_json"),
            "rules": _read_json(row, "rules_json"),
            "decisions": _parse_stored_decisions(row["decisions_json"]),
            "create_source_sha256": row["create_source_sha256"],
            "updated_source_sha256": row["updated_source_sha256"],
        }

    def _begin(self):
        self._staged = None
        self._writer.execute("BEGIN IMMEDIATE")

    def _stage(self, report_sink, report):
        if report_sink is None:
            return None
        try:
            return report_sink(report)
        except BaseException:
            self._discard_staged()
            raise

    def _discard_staged(self):
        staged = self._staged
        if staged is not None:
            discard(staged[1])
            self._staged = None

    def _commit(self):
        if self._simulate_crash:
            raise sqlite3.OperationalError("simulated crash before commit")
        self._writer.execute("COMMIT")

    def revision(self):
        """Committed revision, or ``None`` before the first create."""
        with self._lock:
            row = self._row()
            return None if row is None else int(row["revision"])

    def _writable(self):
        if self.read_only:
            raise WorkspaceError("workspace was opened read-only")

    # ---------------------------------------------------------------- create
    def create(self, data, *, source_sha256=None, report_sink=None):
        self._writable()
        parsed = parse_create(data)
        initial = analyze(parsed["base"], parsed["left"], parsed["right"],
                          parsed["rules"])
        if len(conflict_map(initial)) > MAX_CONFLICTS:
            raise ValueError(
                "create yields more than %d conflicts" % MAX_CONFLICTS
            )
        with self._lock:
            self._begin()
            try:
                if self._row() is not None:
                    raise WorkspaceExistsError("workspace already exists")
                self._writer.execute(
                    "INSERT INTO merge_workspace(id, revision, base_json, "
                    "left_json, right_json, rules_json, decisions_json, "
                    "create_source_sha256, updated_source_sha256) "
                    "VALUES(1, 0, ?, ?, ?, ?, '{}', ?, ?)",
                    (
                        canonical_json(parsed["base"]),
                        canonical_json(parsed["left"]),
                        canonical_json(parsed["right"]),
                        canonical_json(parsed["rules"]),
                        source_sha256,
                        source_sha256,
                    ),
                )
                report = self._inspect_state(
                    {
                        "revision": 0,
                        "base": parsed["base"],
                        "left": parsed["left"],
                        "right": parsed["right"],
                        "rules": parsed["rules"],
                        "decisions": {},
                        "create_source_sha256": source_sha256,
                        "updated_source_sha256": source_sha256,
                    }
                )
                self._staged = self._stage(report_sink, report)
                self._commit()
            except BaseException:
                self._writer.execute("ROLLBACK")
                self._discard_staged()
                raise
        # The report describes exactly the revision this transaction committed
        # instead of re-reading (another process may already have moved on).
        if self._staged is not None:
            publish(*self._staged)
            self._staged = None
        return report

    # --------------------------------------------------------------- refresh
    def refresh(self, data, expected_revision, *, source_sha256=None,
                report_sink=None):
        parsed = parse_refresh(data)
        if not _is_int(expected_revision) or expected_revision < 0:
            raise ValueError("expected_revision must be a non-negative integer")
        self._writable()
        with self._lock:
            self._begin()
            try:
                row = self._row()
                if row is None:
                    raise WorkspaceMissingError("workspace has no committed state")
                state = self._state(row)
                if state["revision"] != expected_revision:
                    raise StaleRevisionError(
                        "stale expected_revision %d; committed revision is %d"
                        % (expected_revision, state["revision"])
                    )
                analysis = analyze(
                    state["base"], parsed["left"], parsed["right"], state["rules"]
                )
                conflicts = conflict_map(analysis)
                if len(conflicts) > 12:
                    raise ValueError(
                        "refresh yields more than %d conflicts" % MAX_CONFLICTS
                    )
                inherited = {
                    conflict_id: decision
                    for conflict_id, decision in state["decisions"].items()
                    if conflict_id in conflicts
                }
                self._writer.execute(
                    "UPDATE merge_workspace SET revision=?, left_json=?, "
                    "right_json=?, decisions_json=?, updated_source_sha256=? "
                    "WHERE id=1",
                    (
                        state["revision"] + 1,
                        canonical_json(parsed["left"]),
                        canonical_json(parsed["right"]),
                        _decisions_payload(inherited),
                        source_sha256,
                    ),
                )
                report = self._inspect_state(
                    {
                        "revision": state["revision"] + 1,
                        "base": state["base"],
                        "left": parsed["left"],
                        "right": parsed["right"],
                        "rules": state["rules"],
                        "decisions": inherited,
                        "create_source_sha256": state["create_source_sha256"],
                        "updated_source_sha256": source_sha256,
                    }
                )
                self._staged = self._stage(report_sink, report)
                self._commit()
            except BaseException:
                self._writer.execute("ROLLBACK")
                self._discard_staged()
                raise
        if self._staged is not None:
            publish(*self._staged)
            self._staged = None
        return report

    # --------------------------------------------------------------- resolve
    def resolve(self, data, expected_revision, *, report_sink=None):
        decisions = parse_resolve(data)
        if not decisions:
            raise ValueError("resolve requires at least one decision")
        if not _is_int(expected_revision) or expected_revision < 0:
            raise ValueError("expected_revision must be a non-negative integer")
        self._writable()
        with self._lock:
            self._begin()
            try:
                row = self._row()
                if row is None:
                    raise WorkspaceMissingError("workspace has no committed state")
                state = self._state(row)
                if state["revision"] != expected_revision:
                    raise StaleRevisionError(
                        "stale expected_revision %d; committed revision is %d"
                        % (expected_revision, state["revision"])
                    )
                analysis = analyze(
                    state["base"], state["left"], state["right"], state["rules"]
                )
                conflicts = conflict_map(analysis)
                merged = {
                    conflict_id: decision
                    for conflict_id, decision in state["decisions"].items()
                    if conflict_id in conflicts
                }
                for decision in decisions:
                    conflict_id = decision["conflict_id"]
                    if conflict_id not in conflicts:
                        raise UnknownConflictError(
                            "unknown conflict id: " + conflict_id
                        )
                    merged[conflict_id] = {
                        "choice": decision["choice"],
                        "tokens": decision["tokens"],
                    }
                feasible, _ = completion_search(analysis, merged)
                if not feasible:
                    raise InfeasibleBatchError(
                        "resolve batch leaves no complete rule-satisfying list"
                    )
                self._writer.execute(
                    "UPDATE merge_workspace SET revision=?, decisions_json=? "
                    "WHERE id=1",
                    (state["revision"] + 1, _decisions_payload(merged)),
                )
                report = self._inspect_state(
                    {
                        "revision": state["revision"] + 1,
                        "base": state["base"],
                        "left": state["left"],
                        "right": state["right"],
                        "rules": state["rules"],
                        "decisions": merged,
                        "create_source_sha256": state["create_source_sha256"],
                        "updated_source_sha256":
                            state["updated_source_sha256"],
                    }
                )
                self._staged = self._stage(report_sink, report)
                self._commit()
            except BaseException:
                self._writer.execute("ROLLBACK")
                self._discard_staged()
                raise
        if self._staged is not None:
            publish(*self._staged)
            self._staged = None
        return report

    # ------------------------------------------------------- read operations
    def _open_reader(self):
        from urllib.request import pathname2url

        uri = "file:" + pathname2url(self.path) + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    def _snapshot(self):
        """Read revision, inputs and decisions from one committed snapshot."""
        connection = self._open_reader()
        try:
            connection.execute("BEGIN")
            row = connection.execute(
                "SELECT * FROM merge_workspace WHERE id = 1"
            ).fetchone()
            if row is None:
                raise WorkspaceMissingError("workspace has no committed state")
            state = self._state(row)
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            finally:
                connection.close()
            raise
        connection.close()
        return state

    def _inspect_state(self, state):
        analysis = analyze(
            state["base"], state["left"], state["right"], state["rules"]
        )
        conflicts = conflict_map(analysis)
        decisions = {
            conflict_id: decision
            for conflict_id, decision in state["decisions"].items()
            if conflict_id in conflicts
        }
        feasible, support = completion_search(analysis, decisions)
        pending = []
        for component in analysis["components"]:
            conflict_id = component.get("conflict_id")
            if conflict_id is None or conflict_id in decisions:
                continue
            pending.append(
                {
                    "conflict_id": conflict_id,
                    "base_range": component["base_range"],
                    "left_projection": component["left_projection"],
                    "right_projection": component["right_projection"],
                    "hunks": component["hunks"],
                    "support": support.get(
                        conflict_id, {"left": False, "right": False}
                    ),
                }
            )
        return {
            "schema_version": 1,
            "revision": state["revision"],
            "input": {
                "base_sha256": analysis["base_sha256"],
                "left_sha256": analysis["left_sha256"],
                "right_sha256": analysis["right_sha256"],
                "rules_sha256": analysis["rules_sha256"],
                "base": analysis["base"],
                "left": analysis["left"],
                "right": analysis["right"],
                "rules": analysis["rules"],
                "create_source_sha256": state["create_source_sha256"],
                "updated_source_sha256": state["updated_source_sha256"],
            },
            "components": [
                dict(component) for component in analysis["components"]
            ],
            "pending": pending,
            "decisions": [
                {
                    "conflict_id": conflict_id,
                    "choice": decision["choice"],
                    "tokens": decision["tokens"],
                }
                for conflict_id, decision in sorted(decisions.items())
            ],
            "feasible": feasible,
            "automatic_count": sum(
                component["kind"] == "automatic"
                for component in analysis["components"]
            ),
            "conflict_count": len(conflicts),
        }

    def inspect(self):
        with self._lock:
            state = self._snapshot()
        return self._inspect_state(state)

    def export(self):
        with self._lock:
            state = self._snapshot()
            analysis = analyze(
                state["base"], state["left"], state["right"], state["rules"]
            )
            conflicts = conflict_map(analysis)
            decisions = {
                conflict_id: decision
                for conflict_id, decision in state["decisions"].items()
                if conflict_id in conflicts
            }
            missing = [
                conflict_id
                for conflict_id in conflicts
                if conflict_id not in decisions
            ]
            if missing:
                raise PendingConflictError(
                    "%d conflict(s) still pending" % len(missing)
                )
            ready, tokens, sources = export_ready(analysis, decisions)
            if not ready:
                raise RulesViolationError(
                    "complete list violates the workspace rules"
                )
            return {
                "schema_version": 1,
                "revision": state["revision"],
                "input": {
                    "base_sha256": analysis["base_sha256"],
                    "left_sha256": analysis["left_sha256"],
                    "right_sha256": analysis["right_sha256"],
                    "rules_sha256": analysis["rules_sha256"],
                },
                "tokens": tokens,
                "sources": sources,
                "rules_satisfied": True,
            }

    # --------------------------------------------------------------- helpers
    def conflict_components(self):
        """Current component view without writing anything."""
        state = self._snapshot()
        analysis = analyze(
            state["base"], state["left"], state["right"], state["rules"]
        )
        return state["revision"], analysis


def open_workspace(path):
    """Open an existing workspace for programming use."""
    return MergeWorkspace(path)


__all__ = [
    "MergeWorkspace",
    "WorkspaceError",
    "StaleRevisionError",
    "WorkspaceExistsError",
    "WorkspaceMissingError",
    "UnknownConflictError",
    "InfeasibleBatchError",
    "PendingConflictError",
    "RulesViolationError",
]
