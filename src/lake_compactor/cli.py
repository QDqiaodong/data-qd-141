"""Command line front-end for the lake compactor.

Examples::

    python -m lake_compactor.cli init --home run/lake
    python -m lake_compactor.cli append --home run/lake \
        --input fixtures/lake/manifest_append1.json --expected-snapshot 0
    python -m lake_compactor.cli compact --home run/lake --partition p1 \
        --files d000001,d000002 --expected-snapshot 1 --target-rows 100
    python -m lake_compactor.cli pin --home run/lake --snapshot 2
    python -m lake_compactor.cli scan --home run/lake --token pin-...
    python -m lake_compactor.cli unpin --home run/lake --token pin-...
    python -m lake_compactor.cli expire --home run/lake --snapshot 1
    python -m lake_compactor.cli gc --home run/lake

Every subcommand prints a JSON report on stdout and accepts ``--output`` for
an atomically published report file.  Failures exit 2 and leave the lake and
any previous report untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from sequence_align.common import discard, publish, save, stage

from .errors import LakeError
from .lake import DEFAULT_TARGET_ROWS, Lake

DESCRIPTION = "Small-file compaction and snapshot-safe reclamation for data lakes"

_MUTATING = ("append", "compact", "pin", "unpin", "expire", "gc")


def _emit(payload) -> int:
    sys.stdout.write(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    )
    sys.stdout.write("\n")
    return 0


def _is_within(path: str, directory: str) -> bool:
    try:
        return os.path.commonpath([path, directory]) == directory
    except ValueError:
        return False


def _check_paths(args) -> None:
    """Reject path conflicts and symlink inputs before touching the lake."""
    home_real = os.path.realpath(args.home)
    source = getattr(args, "input", None)
    output = getattr(args, "output", None)
    if source is not None:
        if Path(source).is_symlink():
            raise ValueError("input cannot be a symlink")
        source_real = os.path.realpath(source)
        if _is_within(source_real, home_real):
            raise ValueError("source path conflicts with the lake home")
        if output is not None and os.path.realpath(output) == source_real:
            raise ValueError("input and output must differ")
    if output is not None:
        if Path(output).is_symlink():
            raise ValueError("output cannot be a symlink")
        if _is_within(os.path.realpath(output), home_real):
            raise ValueError("output path conflicts with the lake home")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lake-compactor", description=DESCRIPTION)
    sub = parser.add_subparsers(dest="command", required=True)

    def base(name, help_text):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--home", required=True,
                       help="tool-owned lake home directory")
        p.add_argument("--output", default=None,
                       help="optional atomic JSON report destination")
        return p

    base("init", "create a lake home with the empty snapshot 0")

    p_append = base("append", "import one manifest of data files and deletes")
    p_append.add_argument("--input", "--manifest", dest="input", required=True,
                          help="strict JSON manifest (data_files + deletes)")
    p_append.add_argument("--expected-snapshot", "--expected_snapshot",
                          dest="expected_snapshot", type=int, required=True)

    p_compact = base("compact", "rewrite selected current data files of one partition")
    p_compact.add_argument("--partition", required=True)
    p_compact.add_argument("--files", required=True,
                           help="comma-separated current data file ids (>=2)")
    p_compact.add_argument("--expected-snapshot", "--expected_snapshot",
                           dest="expected_snapshot", type=int, required=True)
    p_compact.add_argument("--target-rows", "--target_rows", dest="target_rows",
                           type=int, default=DEFAULT_TARGET_ROWS,
                           help="maximum rows per output file")

    p_pin = base("pin", "pin a live snapshot and print a persistent token")
    p_pin.add_argument("--snapshot", type=int, required=True)

    p_scan = base("scan", "read the visible rows of a pinned snapshot")
    p_scan.add_argument("--token", required=True)

    p_unpin = base("unpin", "release a pin token")
    p_unpin.add_argument("--token", required=True)

    p_expire = base("expire", "make a non-current snapshot un-pinnable")
    p_expire.add_argument("--snapshot", type=int, required=True)

    base("gc", "delete tool-owned files unreachable from live snapshots and pins")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    lake = None
    staged = {}
    try:
        _check_paths(args)
        if args.command == "init":
            report = Lake.init(args.home)
            if args.output is not None:
                save(args.output, report)
            return _emit(report)

        lake = Lake(args.home)
        # Mutating subcommands stage their --output report inside the
        # transaction and publish it after COMMIT, so a staging failure rolls
        # the lake back and a lake failure never publishes a report.
        sink = None
        if args.output is not None and args.command in _MUTATING:
            def sink(report):
                staged["pair"] = stage(args.output, report)

        if args.command == "append":
            report = lake.append(args.input, args.expected_snapshot,
                                 report_sink=sink)
        elif args.command == "compact":
            files = [item for item in
                     (part.strip() for part in args.files.split(",")) if item]
            report = lake.compact(
                args.partition, files, args.expected_snapshot,
                target_rows=args.target_rows, report_sink=sink,
            )
        elif args.command == "pin":
            report = lake.pin(args.snapshot, report_sink=sink)
        elif args.command == "scan":
            report = lake.scan(args.token)
            if args.output is not None:
                save(args.output, report)
        elif args.command == "unpin":
            report = lake.unpin(args.token, report_sink=sink)
        elif args.command == "expire":
            report = lake.expire(args.snapshot, report_sink=sink)
        else:
            report = lake.gc(report_sink=sink)

        if staged.get("pair") is not None:
            publish(*staged["pair"])
            staged.clear()
        return _emit(report)
    except (
        LakeError,
        ValueError,
        TypeError,
        KeyError,
        OSError,
        OverflowError,
        RecursionError,
        sqlite3.Error,
    ) as error:
        if staged.get("pair") is not None:
            discard(staged["pair"][1])
        print(f"error: {error}", file=sys.stderr)
        return 2
    finally:
        if lake is not None:
            lake.close()


if __name__ == "__main__":
    raise SystemExit(main())
