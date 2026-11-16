"""Command line front-end for the local CDC history materializer.

Examples:
  cdc-materialize ingest --db run/cdc.db --batch-id b1 --input batch.json
  cdc-materialize query  --db run/cdc.db --sources s1,s2 --entities e1 \\
      --as-of 2026-03-01T00:00:00+00:00 --known-through 2
  cdc-materialize event    --db run/cdc.db --event-id e-001
  cdc-materialize revision --db run/cdc.db --source s1 --entity e1 --revision 3
  cdc-materialize batch    --db run/cdc.db --batch-id b1
"""

from __future__ import annotations

import argparse
import json
import sys

from .store import BatchConflictError, Materializer


def _emit(payload) -> int:
    sys.stdout.write(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    )
    sys.stdout.write("\n")
    return 0


def _load_records(path: str) -> list:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise SystemExit("input file must contain a JSON array of records")
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cdc-materialize")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="ingest one change batch")
    p_ing.add_argument("--db", required=True)
    p_ing.add_argument("--batch-id", required=True)
    p_ing.add_argument("--input", required=True,
                       help="path to a JSON array of change records")
    p_ing.add_argument("--simulate-crash", action="store_true",
                       help="abort before COMMIT (recovery testing)")

    p_q = sub.add_parser("query", help="materialized point-in-time query")
    p_q.add_argument("--db", required=True)
    p_q.add_argument("--sources", required=True,
                     help="comma-separated source names")
    p_q.add_argument("--entities", required=True,
                     help="comma-separated entity names")
    p_q.add_argument("--as-of", required=True, help="business valid time")
    p_q.add_argument("--known-through", type=int, default=None,
                     help="knowledge sequence cutoff (default: current head)")

    p_ev = sub.add_parser("event", help="acceptance verdict for an event_id")
    p_ev.add_argument("--db", required=True)
    p_ev.add_argument("--event-id", required=True)

    p_rev = sub.add_parser("revision", help="verdict for a revision triple")
    p_rev.add_argument("--db", required=True)
    p_rev.add_argument("--source", required=True)
    p_rev.add_argument("--entity", required=True)
    p_rev.add_argument("--revision", type=int, required=True)

    p_bt = sub.add_parser("batch", help="stored result of one batch")
    p_bt.add_argument("--db", required=True)
    p_bt.add_argument("--batch-id", required=True)

    p_head = sub.add_parser("head", help="latest knowledge sequence")
    p_head.add_argument("--db", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with Materializer(
        args.db, simulate_crash_before_commit=getattr(
            args, "simulate_crash", False)
    ) as mat:
        if args.command == "ingest":
            try:
                result = mat.ingest_batch(
                    args.batch_id, _load_records(args.input))
            except BatchConflictError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            return _emit(result)
        if args.command == "query":
            return _emit(mat.query_materialized(
                args.sources.split(","), args.entities.split(","),
                args.as_of, args.known_through))
        if args.command == "event":
            return _emit(mat.lookup_event(args.event_id))
        if args.command == "revision":
            return _emit(mat.lookup_revision(
                args.source, args.entity, args.revision))
        if args.command == "batch":
            result = mat.get_batch(args.batch_id)
            if result is None:
                print(f"error: unknown batch {args.batch_id!r}",
                      file=sys.stderr)
                return 1
            return _emit(result)
        if args.command == "head":
            return _emit({"head_sequence": mat.head_sequence()})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
