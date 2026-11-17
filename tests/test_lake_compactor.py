"""Tests for the lake compactor (small-file compaction + snapshot-safe gc).

The bag oracle in ``BagOracle`` is an independent miniature interpreter of the
contract: it tracks files as plain lists of row dicts and applies deletes and
compactions directly, sharing no code with ``lake_compactor``.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from lake_compactor import (
    AlreadyInitializedError,
    CannotExpireHeadError,
    CorruptionError,
    Lake,
    LakeError,
    NotInitializedError,
    PinTokenError,
    SelectionError,
    SnapshotExpiredError,
    SnapshotNotFoundError,
    StaleSnapshotError,
)

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
FIXTURES = ROOT / "fixtures" / "lake"


def canon(rows):
    """Normalize rows for comparison (JSON round-trip, sorted keys)."""
    return json.loads(json.dumps(rows, sort_keys=True))


def run_cli(*args, env_extra=None, timeout=120):
    env = dict(ENV)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "lake_compactor.cli", *args],
        capture_output=True, text=True, env=env, timeout=timeout,
    )


def write_jsonl(path: Path, rows) -> Path:
    """rows: list of (key, value) tuples or raw strings."""
    lines = []
    for row in rows:
        if isinstance(row, str):
            lines.append(row)
        else:
            key, value = row
            lines.append(json.dumps({"key": key, "value": value},
                                    ensure_ascii=False))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.home = self.dir / "lake"
        self.src = self.dir / "src"
        self.src.mkdir()
        Lake.init(str(self.home))
        self.lake = Lake(str(self.home))

    def tearDown(self):
        self.lake.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------- helpers
    def make_manifest(self, name="m.json", files=(), deletes=()):
        """files: list of (partition, [rows]); writes data files + manifest."""
        entries = []
        for index, (partition, rows) in enumerate(files):
            rel = f"data_{name}_{index}.jsonl"
            write_jsonl(self.src / rel, rows)
            entries.append({"partition": partition, "path": rel})
        manifest = {"data_files": entries, "deletes": list(deletes)}
        path = self.src / name
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        return path

    def append(self, manifest_path, expected=None):
        if expected is None:
            expected = self.lake.head_snapshot()
        return self.lake.append(str(manifest_path), expected)

    def scan_rows(self, snapshot_id=None):
        if snapshot_id is None:
            snapshot_id = self.lake.head_snapshot()
        token = self.lake.pin(snapshot_id)["token"]
        try:
            return self.lake.scan(token)["rows"]
        finally:
            self.lake.unpin(token)

    def bag(self, rows):
        return canon(rows)

    def home_files(self, sub="files"):
        base = self.home / sub
        if not base.is_dir():
            return set()
        return {
            str(p.relative_to(self.home))
            for p in base.rglob("*") if p.is_file()
        }

    def head_file_paths(self):
        info = self.lake.snapshot_info(self.lake.head_snapshot())
        return {f["path"] for f in info["data_files"]}


class BagOracle:
    """Independent interpreter of the compaction/delete contract."""

    def __init__(self):
        self.files = {}    # file_id -> [row]
        self.deletes = []  # [predicate]
        self.seq = 0

    @staticmethod
    def _hit(delete, row):
        return (delete["partition"] == row["partition"]
                and delete["key"] == row["key"]
                and row["data_seq"] <= delete["cutoff"])

    @staticmethod
    def _sort(rows):
        return sorted(rows, key=lambda r: (
            r["partition"], r["key"], r["data_seq"],
            r["origin_file_id"], r["origin_line"]))

    def visible(self):
        rows = [row for rows in self.files.values() for row in rows
                if not any(self._hit(d, row) for d in self.deletes)]
        return self._sort(rows)

    def append(self, file_ids, partitions, rows_per_file, deletes):
        self.seq += 1
        for file_id, partition, rows in zip(file_ids, partitions,
                                            rows_per_file):
            self.files[file_id] = [
                {"partition": partition, "key": key, "value": value,
                 "origin_file_id": file_id, "origin_line": index + 1,
                 "data_seq": self.seq}
                for index, (key, value) in enumerate(rows)
            ]
        self.deletes.extend(dict(d) for d in deletes)

    def compact(self, selected, output_ids, target_rows):
        survivors = []
        for file_id in selected:
            for row in self.files.pop(file_id):
                if not any(self._hit(d, row) for d in self.deletes):
                    survivors.append(row)
        survivors = self._sort(survivors)
        chunks = [survivors[i:i + target_rows]
                  for i in range(0, len(survivors), target_rows)]
        assert len(chunks) == len(output_ids), (len(chunks), output_ids)
        for file_id, chunk in zip(output_ids, chunks):
            self.files[file_id] = chunk
        min_seq = {}
        for rows in self.files.values():
            for row in rows:
                tag = (row["partition"], row["key"])
                if row["data_seq"] < min_seq.get(tag, 1 << 60):
                    min_seq[tag] = row["data_seq"]
        self.deletes = [
            d for d in self.deletes
            if min_seq.get((d["partition"], d["key"]), 1 << 60) <= d["cutoff"]
        ]


# ---------------------------------------------------------------------------
# init / open lifecycle
# ---------------------------------------------------------------------------
class InitLifecycle(LakeCase):
    def test_init_creates_empty_snapshot_zero(self):
        self.assertEqual(self.lake.head_snapshot(), 0)
        self.assertEqual(self.lake.append_seq(), 0)
        info = self.lake.snapshot_info(0)
        self.assertEqual(info["data_files"], [])
        self.assertEqual(info["deletes"], [])
        self.assertEqual(self.scan_rows(0), [])

    def test_reinit_rejected(self):
        with self.assertRaises(AlreadyInitializedError):
            Lake.init(str(self.home))

    def test_open_uninitialized_rejected(self):
        with self.assertRaises(NotInitializedError):
            Lake(str(self.dir / "nowhere"))

    def test_cli_init_and_reinit(self):
        home = self.dir / "cli-lake"
        first = run_cli("init", "--home", str(home))
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(json.loads(first.stdout)["snapshot_id"], 0)
        again = run_cli("init", "--home", str(home))
        self.assertEqual(again.returncode, 2)
        missing = run_cli("append", "--home", str(self.dir / "no-lake"),
                          "--input", "x.json", "--expected-snapshot", "0")
        self.assertEqual(missing.returncode, 2)
        self.assertIn("not initialized", missing.stderr)


# ---------------------------------------------------------------------------
# append validation
# ---------------------------------------------------------------------------
class AppendValidation(LakeCase):
    def append_raw(self, manifest_text, expect_ok=False):
        path = self.src / "manifest.json"
        path.write_text(manifest_text, encoding="utf-8")
        if expect_ok:
            return self.lake.append(str(path), self.lake.head_snapshot())
        with self.assertRaises((ValueError, LakeError)):
            self.lake.append(str(path), self.lake.head_snapshot())
        self.assertEqual(self.lake.head_snapshot(), 0)
        self.assertEqual(self.lake.append_seq(), 0)
        self.assertEqual(self.home_files(), set())

    def test_manifest_must_have_exact_fields(self):
        self.append_raw('{"data_files": [], "deletes": [], "extra": 1}')
        self.append_raw('{"data_files": []}')
        self.append_raw('[1, 2]')
        self.append_raw('{"data_files": {}, "deletes": []}')
        self.append_raw('{"data_files": [], "deletes": {}}')

    def test_manifest_strict_json(self):
        # duplicate keys rejected by the shared strict reader
        self.append_raw('{"data_files": [], "data_files": [], "deletes": []}')
        self.append_raw('{"data_files": [], "deletes": [], "x": NaN}')

    def test_data_file_entry_validation(self):
        self.append_raw('{"data_files": [{"partition": "p"}], "deletes": []}')
        self.append_raw('{"data_files": [{"partition": "p", "path": "x", '
                        '"z": 1}], "deletes": []}')
        self.append_raw('{"data_files": [{"partition": " ", "path": "x"}], '
                        '"deletes": []}')
        self.append_raw('{"data_files": [{"partition": "p", "path": ""}], '
                        '"deletes": []}')
        self.append_raw('{"data_files": [{"partition": "p", "path": "/abs"}], '
                        '"deletes": []}')
        self.append_raw('{"data_files": [{"partition": "p", "path": "missing"'
                        '}], "deletes": []}')

    def test_delete_entry_validation(self):
        base = '{"data_files": [], "deletes": [%s]}'
        self.append_raw(base % '{"partition": "p", "key": "k"}')
        self.append_raw(base % '{"partition": "p", "key": "k", "cutoff": -1}')
        # cutoff 2 exceeds this append's own sequence (1) and is rejected
        self.append_raw(base % '{"partition": "p", "key": "k", "cutoff": 2}')
        self.append_raw(base % '{"partition": "p", "key": "k", "cutoff": true}')
        self.append_raw(base % '{"partition": "p", "key": "k", "cutoff": 0, '
                        '"x": 1}')
        self.append_raw(base % '{"partition": "p", "key": "", "cutoff": 0}')

    def test_cutoff_may_equal_own_append_seq(self):
        # cutoff == this append's seq deletes rows imported by the same append
        manifest = self.make_manifest(files=[("p", [("k", "v1")])],
                                      deletes=[{"partition": "p", "key": "k",
                                                "cutoff": 1}])
        report = self.append(manifest)
        self.assertEqual(report["append_seq"], 1)
        self.assertEqual(self.scan_rows(), [])

    def test_bad_jsonl_lines_fail_whole_append(self):
        bad_lines = [
            '{"key":"a","value":1,"extra":2}',   # extra field
            '{"key":"a"}',                        # missing value
            '{"key":"a","value":1,"value":2}',    # duplicate key
            '{"key":"a","value":NaN}',            # nonfinite literal
            '{"key":"a","value":Infinity}',       # nonfinite literal
            '{"key":"a","value":1e999}',          # overflow -> inf
            '{"key":"","value":1}',               # empty key
            '{"key":" a","value":1}',             # untrimmed key
            '{"key":"a","value":1}',              # (placeholder, replaced below)
            '["not","object"]',                   # not an object
            'not json at all',
            '',                                   # blank line inside file
        ]
        bad_lines[8] = '{"key":"' + "x" * 129 + '","value":1}'  # key too long
        for bad in bad_lines:
            write_jsonl(self.src / "good.jsonl", [("ok", 1)])
            (self.src / "bad.jsonl").write_text(
                '{"key":"b","value":2}\n' + bad + "\n", encoding="utf-8")
            manifest = {
                "data_files": [
                    {"partition": "p", "path": "good.jsonl"},
                    {"partition": "p", "path": "bad.jsonl"},
                ],
                "deletes": [],
            }
            mpath = self.src / "m.json"
            mpath.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ValueError, msg=bad):
                self.lake.append(str(mpath), 0)
            # whole append failed: no files, no snapshot, no sequence consumed
            self.assertEqual(self.lake.head_snapshot(), 0, bad)
            self.assertEqual(self.lake.append_seq(), 0, bad)
            self.assertEqual(self.home_files(), set(), bad)

    def test_append_seq_continues_after_failures(self):
        good = self.make_manifest("g1.json", files=[("p", [("a", 1)])])
        self.append(good)  # seq 1
        bad_manifest = self.src / "bad.json"
        bad_manifest.write_text('{"data_files": [], "deletes": ['
                                '{"partition":"p","key":"a","cutoff":99}]}')
        with self.assertRaises(ValueError):
            self.lake.append(str(bad_manifest), 1)
        report = self.append(self.make_manifest("g2.json",
                                                files=[("p", [("b", 2)])]))
        self.assertEqual(report["append_seq"], 2)  # no gap from the failure
        # compact does not consume the append sequence
        info = self.lake.snapshot_info(self.lake.head_snapshot())
        ids = [f["file_id"] for f in info["data_files"]]
        self.lake.compact("p", ids, self.lake.head_snapshot())
        self.assertEqual(self.lake.append_seq(), 2)
        report = self.append(self.make_manifest("g3.json",
                                                files=[("p", [("c", 3)])]))
        self.assertEqual(report["append_seq"], 3)


# ---------------------------------------------------------------------------
# append semantics
# ---------------------------------------------------------------------------
class AppendSemantics(LakeCase):
    def test_verbatim_copy_and_identity(self):
        rows = [("b", {"x": [1, 2.5, "s", None, True]}), ("a", "v2"), ("b", 7)]
        manifest = self.make_manifest(files=[("p", rows)])
        report = self.append(manifest)
        entry = report["data_files"][0]
        import hashlib
        source_bytes = (self.src / "data_m.json_0.jsonl").read_bytes()
        self.assertEqual(entry["sha256"],
                         hashlib.sha256(source_bytes).hexdigest())
        self.assertEqual(entry["bytes"], len(source_bytes))
        scanned = self.scan_rows()
        self.assertEqual(
            [(r["key"], r["origin_line"], r["data_seq"],
              r["origin_file_id"]) for r in scanned],
            [("a", 2, 1, entry["file_id"]), ("b", 1, 1, entry["file_id"]),
             ("b", 3, 1, entry["file_id"])],
        )
        # duplicate keys are distinct rows, values round-trip exactly
        self.assertEqual([r["value"] for r in scanned],
                         ["v2", {"x": [1, 2.5, "s", None, True]}, 7])

    def test_delete_file_written_per_append_with_stable_ids(self):
        manifest = self.make_manifest(
            files=[("p", [("k", 1)])],
            deletes=[{"partition": "p", "key": "k", "cutoff": 1},
                     {"partition": "p", "key": "z", "cutoff": 0}],
        )
        report = self.append(manifest)
        delete_file = self.home / "files" / "deletes" / (
            report["delete_file"]["file_id"] + ".jsonl")
        lines = [json.loads(line)
                 for line in delete_file.read_text().splitlines()]
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertEqual(set(line), {"delete_id", "partition", "key",
                                         "cutoff"})
        ids = [line["delete_id"] for line in lines]
        self.assertEqual(len(set(ids)), 2)
        info = self.lake.snapshot_info(report["snapshot_id"])
        self.assertEqual([d["delete_id"] for d in info["deletes"]],
                         sorted(ids))

    def test_snapshot_ids_monotonic_across_operations(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)])])
        r1 = self.append(m1)
        info = self.lake.snapshot_info(r1["snapshot_id"])
        ids = [f["file_id"] for f in info["data_files"]] + ["dummy"]
        # need two files: append another then compact the pair
        m2 = self.make_manifest("m2.json", files=[("p", [("b", 2)])])
        r2 = self.append(m2)
        info = self.lake.snapshot_info(r2["snapshot_id"])
        ids = [f["file_id"] for f in info["data_files"]]
        r3 = self.lake.compact("p", ids, r2["snapshot_id"])
        self.assertEqual(
            [r1["snapshot_id"], r2["snapshot_id"], r3["snapshot_id"]],
            [1, 2, 3],
        )

    def test_stale_expected_snapshot_rejected_without_commit(self):
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        self.append(manifest)
        before = self.home_files()
        with self.assertRaises(StaleSnapshotError):
            self.lake.append(str(manifest), 5)
        with self.assertRaises(StaleSnapshotError):
            self.lake.compact("p", ["d000001", "d000002"], 0)
        self.assertEqual(self.lake.head_snapshot(), 1)
        self.assertEqual(self.lake.append_seq(), 1)
        self.assertEqual(self.home_files(), before)


# ---------------------------------------------------------------------------
# delete boundary semantics
# ---------------------------------------------------------------------------
class DeleteBoundary(LakeCase):
    def test_cutoff_boundary_is_inclusive(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("k", "v1")])])
        self.append(m1)  # seq 1
        # cutoff 0 deletes nothing (data_seq starts at 1)
        m2 = self.make_manifest("m2.json", files=[],
                                deletes=[{"partition": "p", "key": "k",
                                          "cutoff": 0}])
        self.append(m2)
        self.assertEqual([r["value"] for r in self.scan_rows()], ["v1"])
        # cutoff 1 deletes the seq-1 row
        m3 = self.make_manifest("m3.json", files=[],
                                deletes=[{"partition": "p", "key": "k",
                                          "cutoff": 1}])
        self.append(m3)
        self.assertEqual(self.scan_rows(), [])
        # a newer row with the same key is unaffected by the old delete
        m4 = self.make_manifest("m4.json", files=[("p", [("k", "v4")])])
        self.append(m4)  # seq 4 > cutoff 1
        self.assertEqual([r["value"] for r in self.scan_rows()], ["v4"])

    def test_deletes_are_partition_scoped(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("k", "vp")]),
                                                  ("q", [("k", "vq")])])
        self.append(m1)
        m2 = self.make_manifest("m2.json", files=[],
                                deletes=[{"partition": "p", "key": "k",
                                          "cutoff": 1}])
        self.append(m2)
        self.assertEqual(
            [(r["partition"], r["value"]) for r in self.scan_rows()],
            [("q", "vq")],
        )


# ---------------------------------------------------------------------------
# compaction
# ---------------------------------------------------------------------------
class Compaction(LakeCase):
    def build(self):
        m1 = self.make_manifest("m1.json", files=[
            ("p", [("b", 1), ("a", 2)]),
            ("p", [("a", 3), ("c", 4)]),
            ("q", [("z", 9)]),
        ])
        self.append(m1)  # snap 1, seq 1
        m2 = self.make_manifest("m2.json", files=[("p", [("b", 5)])],
                                deletes=[{"partition": "p", "key": "c",
                                          "cutoff": 1}])
        self.append(m2)  # snap 2, seq 2
        return self.lake.snapshot_info(2)

    def test_selection_validation(self):
        info = self.build()
        p_files = [f["file_id"] for f in info["data_files"]
                   if f["partition"] == "p"]
        q_files = [f["file_id"] for f in info["data_files"]
                   if f["partition"] == "q"]
        with self.assertRaises(SelectionError):
            self.lake.compact("p", p_files[:1], 2)          # fewer than two
        with self.assertRaises(SelectionError):
            self.lake.compact("p", p_files + [p_files[0]], 2)  # duplicate
        with self.assertRaises(SelectionError):
            self.lake.compact("p", p_files + ["d999999"], 2)   # unknown
        with self.assertRaises(SelectionError):
            self.lake.compact("p", p_files + q_files, 2)       # wrong partition
        with self.assertRaises(SelectionError):
            self.lake.compact("q", q_files + q_files[:1], 2)   # not enough q
        with self.assertRaises(ValueError):
            self.lake.compact("p", p_files, 2, target_rows=0)
        self.assertEqual(self.lake.head_snapshot(), 2)  # nothing committed

    def test_compact_rewrites_only_selected_and_preserves_identity(self):
        info = self.build()
        p_files = sorted(f["file_id"] for f in info["data_files"]
                         if f["partition"] == "p")
        q_file = [f for f in info["data_files"] if f["partition"] == "q"][0]
        q_bytes_before = (self.home / q_file["path"]).read_bytes()
        unselected = p_files[2]
        unselected_bytes_before = (self.home / "files" / "data"
                                   / f"{unselected}.jsonl").read_bytes()

        report = self.lake.compact("p", p_files[:2], 2, target_rows=2)
        self.assertEqual(report["input_rows"], 4)
        # rows: a(2,seq1), a(3,seq1), b(1,seq1), c(4,seq1 deleted) -> 3 rows
        self.assertEqual(report["output_rows"], 3)
        self.assertEqual(report["deleted_rows"], 1)
        self.assertEqual(len(report["outputs"]), 2)  # 2 + 1 chunking
        self.assertEqual([o["rows"] for o in report["outputs"]], [2, 1])

        rows = self.scan_rows()
        got = [(r["partition"], r["key"], r["value"], r["data_seq"],
                r["origin_file_id"], r["origin_line"]) for r in rows]
        self.assertEqual(got, [
            ("p", "a", 2, 1, p_files[0], 2),
            ("p", "a", 3, 1, p_files[1], 1),
            ("p", "b", 1, 1, p_files[0], 1),
            ("p", "b", 5, 2, unselected, 1),
            ("q", "z", 9, 1, q_file["file_id"], 1),
        ])
        # compacted rows keep their original data_seq (not compaction time)
        compacted = [r for r in rows if r["origin_file_id"] in p_files[:2]]
        self.assertTrue(all(r["data_seq"] == 1 for r in compacted))
        # unselected + other-partition files untouched byte-for-byte
        self.assertEqual((self.home / q_file["path"]).read_bytes(),
                         q_bytes_before)
        self.assertEqual((self.home / "files" / "data"
                          / f"{unselected}.jsonl").read_bytes(),
                         unselected_bytes_before)
        # old snapshot is not rewritten
        old = self.lake.snapshot_info(2)
        self.assertEqual(sorted(f["file_id"] for f in old["data_files"]),
                         sorted(p_files + [q_file["file_id"]]))

    def test_recompaction_is_identity_stable(self):
        info = self.build()
        p_files = sorted(f["file_id"] for f in info["data_files"]
                         if f["partition"] == "p")
        before = self.scan_rows()
        r1 = self.lake.compact("p", p_files[:2], 2)
        mid = self.scan_rows()
        self.assertEqual(self.bag(mid), self.bag(before))
        out1 = [o["file_id"] for o in r1["outputs"]]
        r2 = self.lake.compact("p", out1 + [p_files[2]], r1["snapshot_id"],
                               target_rows=1)
        after = self.scan_rows()
        self.assertEqual(self.bag(after), self.bag(before))
        self.assertTrue(all(o["rows"] == 1 for o in r2["outputs"]))

    def test_zero_output_when_all_rows_deleted(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("k", 1)]),
                                                  ("p", [("k", 2)])])
        self.append(m1)
        m2 = self.make_manifest("m2.json", files=[],
                                deletes=[{"partition": "p", "key": "k",
                                          "cutoff": 1}])
        self.append(m2)
        info = self.lake.snapshot_info(2)
        ids = [f["file_id"] for f in info["data_files"]]
        report = self.lake.compact("p", ids, 2)
        self.assertEqual(report["outputs"], [])
        self.assertEqual(report["output_rows"], 0)
        self.assertEqual(self.scan_rows(), [])
        info = self.lake.snapshot_info(report["snapshot_id"])
        self.assertEqual(info["data_files"], [])

    def test_compacted_files_are_self_describing(self):
        self.build()
        info = self.lake.snapshot_info(2)
        p_files = sorted(f["file_id"] for f in info["data_files"]
                         if f["partition"] == "p")
        report = self.lake.compact("p", p_files[:2], 2)
        out = report["outputs"][0]
        lines = (self.home / "files" / "data" / f"{out['file_id']}.jsonl")
        for line in lines.read_text().splitlines():
            obj = json.loads(line)
            self.assertEqual(set(obj), {"key", "value", "origin_file_id",
                                        "origin_line", "data_seq"})


# ---------------------------------------------------------------------------
# partial compaction delete boundary (contract acceptance)
# ---------------------------------------------------------------------------
class PartialCompactionDeleteBoundary(LakeCase):
    def test_delete_spanning_selected_and_unselected_files(self):
        # seq 1: F1 has k=v1, F2 has j=vj ; seq 2: F3 has k=v2
        m1 = self.make_manifest("m1.json", files=[("p", [("k", "v1")]),
                                                  ("p", [("j", "vj")])])
        self.append(m1)
        m2 = self.make_manifest("m2.json", files=[("p", [("k", "v2")])])
        self.append(m2)
        # delete hits F1.k (seq 1) and F3.k (seq 2)
        m3 = self.make_manifest("m3.json", files=[],
                                deletes=[{"partition": "p", "key": "k",
                                          "cutoff": 2}])
        self.append(m3)
        info = self.lake.snapshot_info(3)
        f1, f2, f3 = sorted(f["file_id"] for f in info["data_files"])
        delete_id = info["deletes"][0]["delete_id"]
        self.assertEqual([r["key"] for r in self.scan_rows()], ["j"])

        # compact F1+F2; the delete must survive because it still hits F3
        r = self.lake.compact("p", [f1, f2], 3)
        self.assertEqual(r["deletes_kept"], 1)
        self.assertEqual(r["deletes_dropped"], 0)
        info4 = self.lake.snapshot_info(r["snapshot_id"])
        self.assertEqual([d["delete_id"] for d in info4["deletes"]],
                         [delete_id])
        # the unselected file's old row is still invisible after compaction
        self.assertEqual([r["key"] for r in self.scan_rows()], ["j"])

        # a new row with the same key is visible (its data_seq is newer)
        m4 = self.make_manifest("m4.json", files=[("p", [("k", "v3")])])
        self.append(m4)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["j", "k"])

        # re-compaction keeps the bag and original identities exactly
        before = self.scan_rows()
        info5 = self.lake.snapshot_info(self.lake.head_snapshot())
        g_file = [f["file_id"] for f in info5["data_files"]
                  if f["kind"] == "compact"][0]
        r2 = self.lake.compact("p", [g_file, f3], info5["snapshot_id"])
        after = self.scan_rows()
        self.assertEqual(self.bag(after), self.bag(before))
        # the delete is now dead (nothing unselected matches) and is pruned
        self.assertEqual(r2["deletes_kept"], 0)
        info_last = self.lake.snapshot_info(r2["snapshot_id"])
        self.assertEqual(info_last["deletes"], [])

    def test_delete_not_dropped_when_output_has_no_match(self):
        # delete matches only unselected files; compaction output has no row
        # for its key at all -- the predicate must still survive.
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)
        m2 = self.make_manifest("m2.json", files=[("p", [("c", 3)])],
                                deletes=[{"partition": "p", "key": "c",
                                          "cutoff": 2}])
        self.append(m2)
        info = self.lake.snapshot_info(2)
        f1, f2, f3 = sorted(f["file_id"] for f in info["data_files"])
        r = self.lake.compact("p", [f1, f2], 2)
        # output rows are a,b only; delete on c hits F3 (unselected) -> kept
        self.assertEqual(r["deletes_kept"], 1)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b"])


# ---------------------------------------------------------------------------
# pin / scan / unpin / expire
# ---------------------------------------------------------------------------
class PinScanExpire(LakeCase):
    def populate(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)  # snap 1
        m2 = self.make_manifest("m2.json", files=[("p", [("c", 3)])])
        self.append(m2)  # snap 2

    def test_pin_scan_unpin_cycle(self):
        self.populate()
        token = self.lake.pin(1)["token"]
        rows = self.lake.scan(token)["rows"]
        self.assertEqual([r["key"] for r in rows], ["a", "b"])
        self.lake.unpin(token)
        with self.assertRaises(PinTokenError):
            self.lake.scan(token)
        with self.assertRaises(PinTokenError):
            self.lake.unpin(token)

    def test_pin_survives_process_reopen(self):
        self.populate()
        token = self.lake.pin(1)["token"]
        self.lake.close()
        self.lake = Lake(str(self.home))
        rows = self.lake.scan(token)["rows"]
        self.assertEqual([r["key"] for r in rows], ["a", "b"])

    def test_pin_unknown_and_expired(self):
        self.populate()
        with self.assertRaises(SnapshotNotFoundError):
            self.lake.pin(99)
        self.lake.expire(1)
        with self.assertRaises(SnapshotExpiredError):
            self.lake.pin(1)
        with self.assertRaises(SnapshotExpiredError):
            self.lake.expire(1)  # already expired
        with self.assertRaises(CannotExpireHeadError):
            self.lake.expire(2)  # current head
        with self.assertRaises(SnapshotNotFoundError):
            self.lake.expire(99)

    def test_existing_pin_readable_after_expire(self):
        self.populate()
        token = self.lake.pin(1)["token"]
        self.lake.expire(1)
        rows = self.lake.scan(token)["rows"]
        self.assertEqual([r["key"] for r in rows], ["a", "b"])
        with self.assertRaises(SnapshotExpiredError):
            self.lake.pin(1)  # no new pins

    def test_pin_empty_snapshot_zero(self):
        token = self.lake.pin(0)["token"]
        self.assertEqual(self.lake.scan(token)["rows"], [])


# ---------------------------------------------------------------------------
# gc
# ---------------------------------------------------------------------------
class GcBehaviour(LakeCase):
    def test_gc_full_reclamation_and_shared_files(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("q", [("z", 9)])])
        r1 = self.append(m1)  # snap 1
        m2 = self.make_manifest("m2.json", files=[("p", [("b", 2)])])
        r2 = self.append(m2)  # snap 2 (shares snap-1 files)
        info2 = self.lake.snapshot_info(2)
        p_files = sorted(f["file_id"] for f in info2["data_files"]
                         if f["partition"] == "p")
        r3 = self.lake.compact("p", p_files, 2)  # snap 3
        out_ids = {o["file_id"] for o in r3["outputs"]}

        self.lake.expire(1)
        self.lake.expire(2)
        report = self.lake.gc()
        deleted = {f["path"] for f in report["deleted_files"]}
        # snap-1/2 private files are gone; the q file and compact outputs stay
        self.assertIn("files/data/d000001.jsonl", deleted)   # p file of snap1
        self.assertIn("files/data/d000003.jsonl", deleted)   # p file of snap2
        self.assertNotIn("files/data/d000002.jsonl", deleted)  # q: in head
        remaining = self.home_files("files/data")
        self.assertEqual(remaining, {
            f"files/data/{fid}.jsonl"
            for fid in out_ids | {"d000002"}
        })
        self.assertEqual(self.home_files("files/tmp"), set())
        # head still scans the same bag
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b", "z"])
        # gc is idempotent
        self.assertEqual(self.lake.gc()["deleted_count"], 0)

    def test_gc_never_touches_external_sources(self):
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        self.append(manifest)
        before = {p.name for p in self.src.iterdir()}
        self.lake.gc()
        self.assertEqual({p.name for p in self.src.iterdir()}, before)

    def test_gc_cleans_tmp_orphans(self):
        orphan = self.home / "files" / "tmp" / "d000099.123.part"
        orphan.write_text("partial")
        report = self.lake.gc()
        self.assertFalse(orphan.exists())
        self.assertEqual(report["deleted_count"], 1)

    def test_delete_file_shared_across_snapshots_is_kept(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)  # snap 1, seq 1
        # the delete arrives with the file it will hit (z has data_seq 2)
        m2 = self.make_manifest("m2.json", files=[("p", [("z", 3)])],
                                deletes=[{"partition": "p", "key": "z",
                                          "cutoff": 2}])
        r2 = self.append(m2)  # snap 2, seq 2
        delete_rel = f"files/deletes/{r2['delete_file']['file_id']}.jsonl"
        info = self.lake.snapshot_info(2)
        f1, f2 = sorted(f["file_id"] for f in info["data_files"]
                        if f["file_id"] != "d000003")
        r3 = self.lake.compact("p", [f1, f2], 2)
        self.assertEqual(r3["deletes_kept"], 1)  # still protects d000003
        self.lake.expire(1)
        self.lake.expire(2)
        report = self.lake.gc()
        # the delete file is shared with the new head: it must survive gc
        self.assertNotIn(delete_rel, {f["path"] for f in report["deleted_files"]})
        self.assertTrue((self.home / delete_rel).exists())
        # the z row stays invisible through the compaction
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b"])


# ---------------------------------------------------------------------------
# linked acceptance: pin old head, compact, expire, gc
# ---------------------------------------------------------------------------
class LinkedPinCompactGc(LakeCase):
    def test_old_and_new_tokens_coexist_then_old_files_vanish(self):
        m1 = self.make_manifest("m1.json",
                                files=[("p", [("a", 1), ("b", 2)]),
                                       ("p", [("c", 3)])],
                                deletes=[{"partition": "p", "key": "zzz",
                                          "cutoff": 1}])
        r1 = self.append(m1)  # snap 1
        delete_rel = f"files/deletes/{r1['delete_file']['file_id']}.jsonl"
        old_token = self.lake.pin(1)["token"]
        old_bag = self.scan_rows(1)

        info = self.lake.snapshot_info(1)
        ids = [f["file_id"] for f in info["data_files"]]
        r2 = self.lake.compact("p", ids, 1)  # snap 2
        new_token = self.lake.pin(2)["token"]

        self.lake.expire(1)
        gc1 = self.lake.gc()
        # the pin protects snapshot 1's data files *and* its delete file
        self.assertEqual(gc1["deleted_count"], 0)
        self.assertEqual(self.bag(self.lake.scan(old_token)["rows"]),
                         self.bag(old_bag))
        self.assertEqual(self.bag(self.lake.scan(new_token)["rows"]),
                         self.bag(old_bag))
        for fid in ids:
            self.assertTrue(
                (self.home / "files" / "data" / f"{fid}.jsonl").exists())
        self.assertTrue((self.home / delete_rel).exists())

        self.lake.unpin(old_token)
        gc2 = self.lake.gc()
        deleted = {f["path"] for f in gc2["deleted_files"]}
        self.assertEqual(
            deleted,
            {f"files/data/{fid}.jsonl" for fid in ids} | {delete_rel})
        # new head still reopens and scans
        self.lake.close()
        self.lake = Lake(str(self.home))
        self.assertEqual(self.bag(self.lake.scan(new_token)["rows"]),
                         self.bag(old_bag))


# ---------------------------------------------------------------------------
# corruption
# ---------------------------------------------------------------------------
class Corruption(LakeCase):
    def populate(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1), ("b", 2)]),
                                                  ("p", [("c", 3)])],
                                deletes=[{"partition": "p", "key": "z",
                                          "cutoff": 1}])
        return self.append(m1)

    def test_corrupt_data_file_reports_stable_error(self):
        report = self.populate()
        token = self.lake.pin(1)["token"]
        target = self.home / "files" / "data" / "d000001.jsonl"
        raw = bytearray(target.read_bytes())
        raw[5] ^= 0xFF
        target.write_bytes(bytes(raw))
        messages = []
        for _ in range(2):
            with self.assertRaises(CorruptionError) as ctx:
                self.lake.scan(token)
            messages.append(str(ctx.exception))
        self.assertEqual(messages[0], messages[1])  # stable
        self.assertIn("corrupt lake file d000001", messages[0])
        self.assertIn("sha256 mismatch", messages[0])
        self.assertEqual(report["snapshot_id"], 1)

    def test_missing_file_reports_stable_error(self):
        self.populate()
        token = self.lake.pin(1)["token"]
        (self.home / "files" / "data" / "d000002.jsonl").unlink()
        with self.assertRaises(CorruptionError) as ctx:
            self.lake.scan(token)
        self.assertIn("file is missing", str(ctx.exception))

    def test_corrupt_delete_file_reports_stable_error(self):
        self.populate()
        token = self.lake.pin(1)["token"]
        target = self.home / "files" / "deletes" / "x000001.jsonl"
        raw = bytearray(target.read_bytes())
        raw[-2] ^= 0xFF
        target.write_bytes(bytes(raw))
        with self.assertRaises(CorruptionError) as ctx:
            self.lake.scan(token)
        self.assertIn("corrupt lake file x000001", str(ctx.exception))

    def test_corruption_fails_compaction(self):
        self.populate()
        target = self.home / "files" / "data" / "d000001.jsonl"
        target.write_bytes(target.read_bytes() + b" ")
        with self.assertRaises(CorruptionError):
            self.lake.compact("p", ["d000001", "d000002"], 1)
        self.assertEqual(self.lake.head_snapshot(), 1)


# ---------------------------------------------------------------------------
# path safety
# ---------------------------------------------------------------------------
class PathSafety(LakeCase):
    def test_symlink_inputs_rejected(self):
        write_jsonl(self.src / "real.jsonl", [("a", 1)])
        (self.src / "link.jsonl").symlink_to(self.src / "real.jsonl")
        manifest = {"data_files": [{"partition": "p", "path": "link.jsonl"}],
                    "deletes": []}
        mpath = self.src / "m.json"
        mpath.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            self.lake.append(str(mpath), 0)
        # manifest itself a symlink
        (self.src / "mlink.json").symlink_to(mpath)
        with self.assertRaises(ValueError):
            self.lake.append(str(self.src / "mlink.json"), 0)
        self.assertEqual(self.lake.head_snapshot(), 0)

    def test_source_inside_home_rejected(self):
        inside = self.home / "sneaky.jsonl"
        write_jsonl(inside, [("a", 1)])
        manifest = {"data_files": [{"partition": "p", "path": "sneaky.jsonl"}],
                    "deletes": []}
        mpath = self.home / "m.json"
        mpath.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            self.lake.append(str(mpath), 0)

    def test_cli_path_conflicts(self):
        home = self.dir / "cli-lake"
        self.assertEqual(run_cli("init", "--home", str(home)).returncode, 0)
        write_jsonl(self.src / "d.jsonl", [("a", 1)])
        manifest = {"data_files": [{"partition": "p", "path": "d.jsonl"}],
                    "deletes": []}
        mpath = self.src / "m.json"
        mpath.write_text(json.dumps(manifest))
        # manifest inside home
        in_home = run_cli("append", "--home", str(home),
                          "--input", str(home / "m.json"),
                          "--expected-snapshot", "0")
        self.assertEqual(in_home.returncode, 2)
        # output inside home
        out_home = run_cli("append", "--home", str(home),
                           "--input", str(mpath), "--expected-snapshot", "0",
                           "--output", str(home / "report.json"))
        self.assertEqual(out_home.returncode, 2)
        # input == output
        same = run_cli("append", "--home", str(home), "--input", str(mpath),
                       "--expected-snapshot", "0", "--output", str(mpath))
        self.assertEqual(same.returncode, 2)
        # symlink manifest via CLI
        (self.src / "mlink.json").symlink_to(mpath)
        linked = run_cli("append", "--home", str(home),
                         "--input", str(self.src / "mlink.json"),
                         "--expected-snapshot", "0")
        self.assertEqual(linked.returncode, 2)


# ---------------------------------------------------------------------------
# report publication
# ---------------------------------------------------------------------------
class ReportPublication(LakeCase):
    def test_output_written_atomically_on_success(self):
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        out = self.dir / "report.json"
        result = run_cli("append", "--home", str(self.home),
                         "--input", str(manifest), "--expected-snapshot", "0",
                         "--output", str(out))
        self.assertEqual(result.returncode, 0, result.stderr)
        on_disk = json.loads(out.read_text())
        on_stdout = json.loads(result.stdout)
        self.assertEqual(on_disk, on_stdout)
        self.assertEqual(on_disk["snapshot_id"], 1)

    def test_no_report_on_failure_and_old_report_kept(self):
        out = self.dir / "report.json"
        out.write_text('{"previous": true}')
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        stale = run_cli("append", "--home", str(self.home),
                        "--input", str(manifest), "--expected-snapshot", "9",
                        "--output", str(out))
        self.assertEqual(stale.returncode, 2)
        self.assertIn("stale", stale.stderr)
        self.assertEqual(json.loads(out.read_text()), {"previous": True})
        bad = self.src / "bad.json"
        bad.write_text('{"data_files": "nope", "deletes": []}')
        failed = run_cli("append", "--home", str(self.home),
                         "--input", str(bad), "--expected-snapshot", "0",
                         "--output", str(out))
        self.assertEqual(failed.returncode, 2)
        self.assertEqual(json.loads(out.read_text()), {"previous": True})

    def test_scan_output_file(self):
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        self.append(manifest)
        token = self.lake.pin(1)["token"]
        out = self.dir / "scan.json"
        result = run_cli("scan", "--home", str(self.home),
                         "--token", token, "--output", str(out))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(out.read_text())["row_count"], 1)


# ---------------------------------------------------------------------------
# randomized oracle comparison
# ---------------------------------------------------------------------------
class RandomizedOracle(LakeCase):
    def test_random_operations_match_bag_oracle(self):
        rng = random.Random(20261116)
        oracle = BagOracle()
        partitions = ["p1", "p2"]
        keys = ["a", "b", "c", "d"]
        values = [1, "s", None, True, {"x": 1}, [1, 2], 2.5]
        pinned = []
        manifests = 0
        source_files = 0

        def check_head():
            token = self.lake.pin(self.lake.head_snapshot())["token"]
            try:
                got = self.lake.scan(token)["rows"]
            finally:
                self.lake.unpin(token)
            self.assertEqual(canon(got), canon(oracle.visible()))

        for step in range(36):
            op = rng.choice(["append", "append", "append", "compact",
                             "compact", "expire_gc", "pin"])
            if op == "append":
                manifests += 1
                nfiles = rng.randint(1, 3)
                spec, rows_per_file, parts = [], [], []
                for index in range(nfiles):
                    part = rng.choice(partitions)
                    rows = [(rng.choice(keys), rng.choice(values))
                            for _ in range(rng.randint(0, 4))]
                    parts.append(part)
                    rows_per_file.append(rows)
                    spec.append((part, rows))
                deletes = [
                    {"partition": rng.choice(partitions),
                     "key": rng.choice(keys),
                     "cutoff": rng.randint(0, self.lake.append_seq() + 1)}
                    for _ in range(rng.randint(0, 2))
                ]
                manifest = self.make_manifest(f"rm{manifests}.json",
                                              files=spec, deletes=deletes)
                source_files += nfiles
                report = self.append(manifest)
                oracle.append(
                    [f["file_id"] for f in report["data_files"]],
                    parts, rows_per_file, deletes,
                )
            elif op == "compact":
                head = self.lake.head_snapshot()
                info = self.lake.snapshot_info(head)
                by_part = {}
                for f in info["data_files"]:
                    by_part.setdefault(f["partition"], []).append(f["file_id"])
                candidates = [p for p, ids in by_part.items() if len(ids) >= 2]
                if not candidates:
                    continue
                part = rng.choice(candidates)
                chosen = rng.sample(by_part[part],
                                    rng.randint(2, len(by_part[part])))
                target = rng.randint(1, 3)
                report = self.lake.compact(part, chosen, head,
                                           target_rows=target)
                oracle.compact(sorted(chosen),
                               [o["file_id"] for o in report["outputs"]],
                               target)
            elif op == "expire_gc":
                head = self.lake.head_snapshot()
                for sid in range(head):
                    try:
                        self.lake.expire(sid)
                    except LakeError:
                        pass
                report = self.lake.gc()
                # gc may never delete anything the current head needs
                self.assertTrue(
                    self.head_file_paths().isdisjoint(
                        f["path"] for f in report["deleted_files"]))
            else:  # pin
                token = self.lake.pin(self.lake.head_snapshot())["token"]
                pinned.append((token, canon(oracle.visible())))
            check_head()

        # every pinned snapshot still reads exactly its pinned bag
        for token, expected in pinned:
            got = self.lake.scan(token)["rows"]
            self.assertEqual(canon(got), expected)
        # external source files were never touched by gc
        self.assertEqual(len(list(self.src.glob("*.jsonl"))), source_files)
        for token, _ in pinned:
            self.lake.unpin(token)


# ---------------------------------------------------------------------------
# concurrency
# ---------------------------------------------------------------------------
class Concurrency(LakeCase):
    def test_two_processes_append_one_winner(self):
        manifest = self.make_manifest(files=[("p", [("a", 1)])])
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "lake_compactor.cli", "append",
                 "--home", str(self.home), "--input", str(manifest),
                 "--expected-snapshot", "0"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=ENV)
            for _ in range(2)
        ]
        results = [p.communicate() + (p.returncode,) for p in procs]
        codes = sorted(r[2] for r in results)
        self.assertEqual(codes, [0, 2], results)
        loser = [r for r in results if r[2] == 2][0]
        self.assertIn("stale", loser[1])
        self.assertEqual(self.lake.head_snapshot(), 1)
        self.assertEqual(self.lake.append_seq(), 1)

    def test_pin_expire_race_yields_token_or_clear_error(self):
        for _ in range(5):
            with tempfile.TemporaryDirectory() as td:
                home = Path(td) / "lake"
                Lake.init(str(home))
                lake = Lake(str(home))
                src = Path(td)
                write_jsonl(src / "d1.jsonl", [("a", 1)])
                for index, seq in enumerate((0, 1)):
                    (src / f"m{index}.json").write_text(json.dumps({
                        "data_files": [], "deletes": []}))
                    lake.append(str(src / f"m{index}.json"), seq)
                lake.close()
                pin = subprocess.Popen(
                    [sys.executable, "-m", "lake_compactor.cli", "pin",
                     "--home", str(home), "--snapshot", "1"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, env=ENV)
                expire = subprocess.Popen(
                    [sys.executable, "-m", "lake_compactor.cli", "expire",
                     "--home", str(home), "--snapshot", "1"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, env=ENV)
                pin_out, pin_err = pin.communicate()
                expire_out, expire_err = expire.communicate()
                self.assertEqual(expire.returncode, 0, expire_err)
                if pin.returncode == 0:
                    token = json.loads(pin_out)["token"]
                    lake = Lake(str(home))
                    self.assertEqual(lake.scan(token)["snapshot_id"], 1)
                    lake.close()
                else:
                    self.assertEqual(pin.returncode, 2)
                    self.assertIn("expired", pin_err)

    def test_scan_paused_between_files_immune_to_gc(self):
        # snapshot 1 has two data files; pin it, move the head, expire it,
        # then scan slowly while gc runs: the pin must fully protect it.
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)
        token = self.lake.pin(1)["token"]
        m2 = self.make_manifest("m2.json", files=[("p", [("c", 3)])])
        self.append(m2)
        self.lake.expire(1)
        self.lake.close()

        scan = subprocess.Popen(
            [sys.executable, "-m", "lake_compactor.cli", "scan",
             "--home", str(self.home), "--token", token],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env={**ENV, "LAKE_COMPACTOR_SCAN_FILE_DELAY": "1.0"})
        gc = run_cli("gc", "--home", str(self.home))
        self.assertEqual(gc.returncode, 0, gc.stderr)
        out, err = scan.communicate(timeout=60)
        self.assertEqual(scan.returncode, 0, err)
        rows = json.loads(out)["rows"]
        self.assertEqual([r["key"] for r in rows], ["a", "b"])
        # the pinned data files were protected; only the unreferenced empty
        # delete files of the two appends were collectable
        deleted = {f["path"] for f in json.loads(gc.stdout)["deleted_files"]}
        self.assertTrue(deleted <= {"files/deletes/x000001.jsonl",
                                    "files/deletes/x000002.jsonl"}, deleted)
        self.assertTrue(
            (self.home / "files" / "data" / "d000001.jsonl").exists())
        self.assertTrue(
            (self.home / "files" / "data" / "d000002.jsonl").exists())
        self.lake = Lake(str(self.home))

    def test_threaded_append_gc_and_scan(self):
        errors = []
        oracle_rows = []
        oracle_lock = threading.Lock()
        stop = threading.Event()

        def writer():
            try:
                lake = Lake(str(self.home))
                for index in range(6):
                    src = self.src / f"w{index}.jsonl"
                    write_jsonl(src, [(f"k{index}", index)])
                    manifest = self.src / f"w{index}.json"
                    manifest.write_text(json.dumps({
                        "data_files": [{"partition": "p",
                                        "path": src.name}],
                        "deletes": []}))
                    while True:
                        try:
                            lake.append(str(manifest), lake.head_snapshot())
                            break
                        except StaleSnapshotError:
                            continue
                    with oracle_lock:
                        oracle_rows.append((f"k{index}", index))
                lake.close()
            except Exception as exc:  # noqa: BLE001 - reported below
                errors.append(exc)
            finally:
                stop.set()

        def recycler():
            lake = Lake(str(self.home))
            try:
                while not stop.is_set():
                    head = lake.head_snapshot()
                    for sid in range(head):
                        try:
                            lake.expire(sid)
                        except LakeError:
                            pass
                    lake.gc()
                    time.sleep(0.01)
            finally:
                lake.close()

        def reader():
            lake = Lake(str(self.home))
            try:
                while not stop.is_set():
                    try:
                        token = lake.pin(lake.head_snapshot())["token"]
                    except LakeError:
                        continue
                    try:
                        lake.scan(token)
                    finally:
                        lake.unpin(token)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                lake.close()

        threads = [threading.Thread(target=writer),
                   threading.Thread(target=recycler),
                   threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)
        self.assertFalse(errors, errors)
        self.lake.gc()
        got = {(r["key"], r["value"]) for r in self.scan_rows()}
        self.assertEqual(got, set(oracle_rows))


# ---------------------------------------------------------------------------
# real process kills and reopen recovery
# ---------------------------------------------------------------------------
class CrashRecovery(LakeCase):
    def freeze_and_kill(self, args, env_var, marker: Path):
        env = dict(ENV)
        env[env_var] = str(marker)
        proc = subprocess.Popen(
            [sys.executable, "-m", "lake_compactor.cli", *args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=env)
        deadline = time.time() + 60
        while not marker.exists():
            if proc.poll() is not None:
                self.fail(f"process exited before freeze point: "
                          f"{proc.stderr.read()}")
            if time.time() > deadline:
                proc.kill()
                self.fail("freeze marker never appeared")
            time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        self.assertNotEqual(proc.returncode, 0)
        proc.stdout.close()
        proc.stderr.close()

    def test_kill_after_files_before_commit(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)])])
        self.append(m1)  # snap 1, seq 1
        self.lake.close()
        m2 = self.make_manifest("m2.json", files=[("p", [("b", 2)]),
                                                  ("q", [("z", 9)])],
                                deletes=[{"partition": "p", "key": "a",
                                          "cutoff": 1}])
        marker = self.dir / "frozen"
        self.freeze_and_kill(
            ["append", "--home", str(self.home), "--input", str(m2),
             "--expected-snapshot", "1"],
            "LAKE_COMPACTOR_FREEZE_AFTER_FILES", marker)

        # reopen: the old head is fully readable, nothing half-committed
        self.lake = Lake(str(self.home))
        self.assertEqual(self.lake.head_snapshot(), 1)
        self.assertEqual(self.lake.append_seq(), 1)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a"])
        # uncommitted orphans are on disk (tmp and/or renamed files)
        self.assertTrue(self.home_files("files/tmp")
                        or len(self.home_files("files/data")) > 1)
        report = self.lake.gc()
        self.assertGreaterEqual(report["deleted_count"], 1)
        self.assertEqual(self.home_files("files/tmp"), set())
        self.assertEqual(self.home_files("files/data"),
                         {"files/data/d000001.jsonl"})
        # the failed append consumed no sequence: the retry gets seq 2
        retry = self.append(m2)
        self.assertEqual(retry["append_seq"], 2)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["b", "z"])

    def test_kill_compact_after_files_before_commit(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)
        self.lake.close()
        marker = self.dir / "frozen"
        self.freeze_and_kill(
            ["compact", "--home", str(self.home), "--partition", "p",
             "--files", "d000001,d000002", "--expected-snapshot", "1"],
            "LAKE_COMPACTOR_FREEZE_AFTER_FILES", marker)
        self.lake = Lake(str(self.home))
        self.assertEqual(self.lake.head_snapshot(), 1)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b"])
        self.lake.gc()
        self.assertEqual(self.home_files("files/tmp"), set())
        self.assertEqual(len(self.home_files("files/data")), 2)
        # compaction still works afterwards
        report = self.lake.compact("p", ["d000001", "d000002"], 1)
        self.assertEqual(report["snapshot_id"], 2)

    def test_kill_after_commit_before_cleanup(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)
        self.lake.close()
        marker = self.dir / "frozen"
        self.freeze_and_kill(
            ["compact", "--home", str(self.home), "--partition", "p",
             "--files", "d000001,d000002", "--expected-snapshot", "1"],
            "LAKE_COMPACTOR_FREEZE_AFTER_COMMIT", marker)
        # reopen: the new head was committed and is fully readable; the old
        # files are still on disk (cleanup is gc's job, not compact's)
        self.lake = Lake(str(self.home))
        self.assertEqual(self.lake.head_snapshot(), 2)
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b"])
        self.assertEqual(len(self.home_files("files/data")), 3)
        self.lake.expire(1)
        report = self.lake.gc()
        self.assertEqual({f["path"] for f in report["deleted_files"]},
                         {"files/data/d000001.jsonl",
                          "files/data/d000002.jsonl",
                          "files/deletes/x000001.jsonl"})
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b"])

    def test_kill_gc_mid_delete_then_reconcile(self):
        m1 = self.make_manifest("m1.json", files=[("p", [("a", 1)]),
                                                  ("p", [("b", 2)])])
        self.append(m1)
        m2 = self.make_manifest("m2.json", files=[("p", [("c", 3)])])
        self.append(m2)
        self.lake.expire(1)
        self.lake.close()
        marker = self.dir / "frozen"
        self.freeze_and_kill(
            ["gc", "--home", str(self.home)],
            "LAKE_COMPACTOR_FREEZE_AFTER_GC_DELETE", marker)
        # reopen: head reads fine; a rerun of gc converges to a clean state
        self.lake = Lake(str(self.home))
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b", "c"])
        self.lake.gc()
        self.assertEqual(self.lake.gc()["deleted_count"], 0)
        self.assertEqual(self.home_files("files/data"),
                         {"files/data/d000001.jsonl",
                          "files/data/d000002.jsonl",
                          "files/data/d000003.jsonl"})
        self.assertEqual([r["key"] for r in self.scan_rows()], ["a", "b", "c"])


# ---------------------------------------------------------------------------
# fixtures walkthrough (README flow through the real CLI)
# ---------------------------------------------------------------------------
class FixturesWalkthrough(unittest.TestCase):
    def test_readme_flow(self):
        with tempfile.TemporaryDirectory() as td:
            home = Path(td) / "lake"

            def cli(*args):
                result = run_cli(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            cli("init", "--home", str(home))
            r1 = cli("append", "--home", str(home),
                     "--input", str(FIXTURES / "manifest_append1.json"),
                     "--expected-snapshot", "0")
            self.assertEqual(r1["append_seq"], 1)
            r2 = cli("append", "--home", str(home),
                     "--input", str(FIXTURES / "manifest_append2.json"),
                     "--expected-snapshot", "1")
            self.assertEqual(r2["append_seq"], 2)
            token2 = cli("pin", "--home", str(home),
                         "--snapshot", "2")["token"]
            rows = cli("scan", "--home", str(home), "--token", token2)["rows"]
            keys = [r["key"] for r in rows]
            # order-1 and order-4 are deleted; everything else visible
            self.assertEqual(keys, ["customer-1", "customer-2", "order-2",
                                    "order-2", "order-3", "order-5"])
            r3 = cli("compact", "--home", str(home), "--partition", "orders",
                     "--files", "d000001,d000002", "--expected-snapshot", "2",
                     "--target-rows", "100")
            self.assertEqual(r3["output_rows"], 2)
            token3 = cli("pin", "--home", str(home),
                         "--snapshot", "3")["token"]
            rows3 = cli("scan", "--home", str(home), "--token", token3)["rows"]
            self.assertEqual([r["key"] for r in rows3], keys)
            cli("expire", "--home", str(home), "--snapshot", "1")
            cli("expire", "--home", str(home), "--snapshot", "2")
            cli("unpin", "--home", str(home), "--token", token2)
            report = cli("gc", "--home", str(home))
            self.assertGreaterEqual(report["deleted_count"], 1)
            # old token is invalid now; new head token still scans
            gone = run_cli("scan", "--home", str(home), "--token", token2)
            self.assertEqual(gone.returncode, 2)
            rows4 = cli("scan", "--home", str(home), "--token", token3)["rows"]
            self.assertEqual([r["key"] for r in rows4], keys)


if __name__ == "__main__":
    unittest.main()
