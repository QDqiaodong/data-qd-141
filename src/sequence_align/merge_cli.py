"""Command line front-end for the three-way sequence merge workbench.

Examples::

    python -m sequence_align.merge_cli create --db run/merge.db \
        --input fixtures/merge.json --output run/merge-inspect.json
    python -m sequence_align.merge_cli refresh --db run/merge.db \
        --input fixtures/merge_refresh.json --expected-revision 0
    python -m sequence_align.merge_cli resolve --db run/merge.db \
        --input decisions.json --expected-revision 1
    python -m sequence_align.merge_cli inspect --db run/merge.db
    python -m sequence_align.merge_cli export --db run/merge.db \
        --output run/merged.json

Every subcommand accepts ``--output``; when omitted the JSON report is
printed to stdout.  ``inspect``/``export`` never write the workspace.
Failures exit 2 and leave both the workspace and an existing output file
untouched.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .common import read, save, stage
from .merge import parse_create, parse_refresh, parse_resolve
from .workspace import MergeWorkspace, WorkspaceError, WorkspaceMissingError

DESCRIPTION = "Three-way sequence merge review workspace"


def _same_path(first, second):
    return Path(first).resolve() == Path(second).resolve()


def _input_with_expected(path):
    data, source_sha256 = read(path)
    expected = None
    if isinstance(data, dict) and "expected_revision" in data:
        expected = data["expected_revision"]
        data = dict(data)
        del data["expected_revision"]
    return data, expected, source_sha256


def _resolve_expected(flag_value, embedded, required):
    if flag_value is not None and embedded is not None and flag_value != embedded:
        raise ValueError("conflicting expected_revision values")
    value = flag_value if flag_value is not None else embedded
    if value is None and required:
        raise ValueError("expected_revision is required")
    return value


def _report(args, payload):
    if args.output is not None:
        save(args.output, payload)
    sys.stdout.write(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2)
    )
    sys.stdout.write("\n")
    return 0


def _check_paths(args, has_input):
    db = Path(args.db)
    if has_input and _same_path(args.input, args.db):
        raise ValueError("database path cannot be the JSON input")
    if args.output is not None and _same_path(args.output, args.db):
        raise ValueError("database path cannot be the JSON output")
    if has_input and args.output is not None and _same_path(args.input, args.output):
        raise ValueError("input and output must differ")
    if args.output is not None and Path(args.output).is_symlink():
        raise ValueError("output cannot be a symlink")
    return db


def _decisions(data):
    """Accept the canonical object form, a decision map or one decision.

    Canonical: ``{"decisions": [{"conflict_id", "choice", "tokens"?}, ...]}``.
    Also accepted: ``{"decisions": {id: "left" | ["manual", ...]}}`` and a
    single ``{"conflict_id": ..., "choice": ...}`` object.
    """
    if isinstance(data, dict) and set(data) == {"decisions"}:
        value = data["decisions"]
        if isinstance(value, dict):
            items = []
            for conflict_id, choice in value.items():
                if isinstance(choice, str):
                    items.append({"conflict_id": conflict_id,
                                  "choice": choice})
                elif isinstance(choice, list):
                    items.append({"conflict_id": conflict_id,
                                  "choice": "manual", "tokens": choice})
                else:
                    raise ValueError("decision value must be left/right or tokens")
            return {"decisions": items}
    if isinstance(data, dict) and "conflict_id" in data and "choice" in data:
        return {"decisions": [data]}
    return data


def build_parser():
    parser = argparse.ArgumentParser(prog="sequence-merge", description=DESCRIPTION)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(name, help_text, input_required=False, output=True):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--db", "--database", "--workspace", dest="db",
                       required=True,
                       help="SQLite workspace path (never a JSON input/output)")
        if input_required:
            p.add_argument("--input", "--source", dest="input", required=True,
                           help="strict JSON input document")
        if output:
            p.add_argument("--output", "--report", dest="output", default=None,
                           help="optional atomic JSON report destination")
        return p

    p_create = common("create", "initialise a workspace at revision 0",
                      input_required=True)
    p_create.add_argument("--simulate-crash", action="store_true",
                          help="roll back just before COMMIT (testing)")

    p_refresh = common("refresh", "replace left/right inputs",
                       input_required=True)
    p_refresh.add_argument("--expected-revision", "--expected_revision",
                           dest="expected_revision", type=int, default=None)
    p_refresh.add_argument("--simulate-crash", action="store_true")

    p_resolve = common("resolve", "add or replace conflict decisions",
                       input_required=True)
    p_resolve.add_argument("--expected-revision", "--expected_revision",
                           dest="expected_revision", type=int, default=None)
    p_resolve.add_argument("--simulate-crash", action="store_true")

    common("inspect", "show revision, components, pending work and decisions")
    common("export", "emit the complete merged token list")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    has_input = getattr(args, "input", None) is not None
    workspace = None
    try:
        _check_paths(args, has_input)
        # Parse and validate the JSON document *before* opening (and possibly
        # creating) the workspace, so a bad document never leaves a file.
        data = source_sha256 = embedded = None
        if has_input:
            data, embedded, source_sha256 = _input_with_expected(args.input)
        # Structural validation happens before the SQLite file is opened, so a
        # rejected document never even creates the workspace file.
        expected = None
        if args.command in ("refresh", "resolve"):
            expected = _resolve_expected(
                args.expected_revision, embedded, True
            )
            if not Path(args.db).exists():
                raise WorkspaceMissingError("workspace has no committed state")
        if args.command == "create":
            parsed = parse_create(data)
            # Refuse an oversized component set before the workspace file is
            # created, so a rejected document leaves no trace at all.
            from .merge import MAX_CONFLICTS, analyze as analyze_merge
            from .merge import conflict_map as conflicts_of
            if len(conflicts_of(analyze_merge(
                    parsed["base"], parsed["left"], parsed["right"],
                    parsed["rules"]))) > MAX_CONFLICTS:
                raise ValueError(
                    "create yields more than %d conflicts" % MAX_CONFLICTS)
        elif args.command == "refresh":
            parse_refresh(data)
        elif args.command == "resolve":
            parse_resolve(_decisions(data))
        read_only = args.command in ("inspect", "export")
        workspace = MergeWorkspace(
            args.db,
            simulate_crash_before_commit=getattr(args, "simulate_crash", False),
            read_only=read_only,
        )
        # For mutating subcommands the report is staged to a temporary sibling
        # and only published after COMMIT; if staging fails the transaction
        # rolls back, so the workspace and the old report stay untouched.
        sink = None
        if args.command in ("create", "refresh", "resolve") \
                and args.output is not None:
            sink = lambda report: stage(args.output, report)
        if args.command == "create":
            if embedded is not None:
                raise ValueError("create does not accept expected_revision")
            payload = workspace.create(
                data, source_sha256=source_sha256, report_sink=sink)
        elif args.command == "refresh":
            payload = workspace.refresh(
                data, expected, source_sha256=source_sha256,
                report_sink=sink,
            )
        elif args.command == "resolve":
            payload = workspace.resolve(
                _decisions(data), expected, report_sink=sink)
        elif args.command == "inspect":
            payload = workspace.inspect()
        else:
            payload = workspace.export()
        if sink is None:
            return _report(args, payload)
        # The mutation already published the report atomically.
        sys.stdout.write(
            json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2)
        )
        sys.stdout.write("\n")
        return 0
    except (
        WorkspaceError,
        ValueError,
        TypeError,
        KeyError,
        OSError,
        OverflowError,
        RecursionError,
        sqlite3.Error,
    ) as error:
        print(str(error), file=sys.stderr)
        return 2
    finally:
        if workspace is not None:
            workspace.close()


if __name__ == "__main__":
    raise SystemExit(main())
