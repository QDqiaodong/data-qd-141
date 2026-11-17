"""Small-file compaction and snapshot-safe reclamation for offline data lakes.

A lake lives in one tool-owned *home* directory::

    <home>/catalog.db          SQLite catalog (WAL, synchronous=FULL)
    <home>/files/data/         immutable data JSONL files (d000001.jsonl, ...)
    <home>/files/deletes/      immutable delete JSONL files (x000001.jsonl, ...)
    <home>/files/tmp/          in-flight writes (never catalog-referenced)

Publication protocol
--------------------
Every mutation (append/compact) runs inside one ``BEGIN IMMEDIATE``
transaction: new files are written under ``files/tmp``, fsynced, renamed into
their final immutable location, the catalog rows and the new snapshot head are
inserted, and the transaction commits.  The COMMIT is the single atomic
publication point: before it readers observe the old head, after it the new
head whose files are already fully on disk.  A crash before COMMIT leaves
rolled-back catalog state plus orphan files that no snapshot references; a
crash after COMMIT leaves a fully readable new head.  Orphans are reclaimed by
``gc`` -- SQLite rollback alone is never claimed to undo external file writes.

Because file creation happens *inside* the write transaction, ``gc`` (which
itself takes the write lock) can never observe a live writer's half-published
files: when ``gc`` runs, any on-disk file missing from the catalog belongs to
a dead writer and is safe to unlink.  No grace periods, no leases.

Dependency tracking
-------------------
A snapshot's dependencies are its data files (``snapshot_data_files``) and its
effective delete entries (``snapshot_deletes`` -> delete file via
``delete_entries``).  ``gc`` protects exactly the files reachable from the
current head, every unexpired snapshot and every pinned snapshot, and deletes
every other file under ``files/``.  ``origin_file_id`` inside compacted rows
is provenance only -- it never keeps a file alive.

Correctness costs (accepted, documented)
----------------------------------------
* Writers hold the SQLite write lock across file copies: appends and
  compactions serialize, and a long copy delays concurrent pin/expire/gc
  commits (they queue on the database lock).  Scans never block: they read
  the catalog through a WAL read-only snapshot and then read content files
  directly.
* Compaction re-reads every unselected head file to decide delete survival,
  so its cost is O(lake size), not O(selected files).  That is the price of
  pruning deletes exactly ("keep exactly the predicates that still hit a
  stored row of an unselected file") instead of approximating.
* Reclamation happens only when an operator runs ``gc``; there is no
  background thread.  Pins have no lease: a forgotten pin protects its
  snapshot forever, by design.
* Snapshot header rows are kept forever (one small row per append/compact)
  so that ``pin`` can distinguish "unknown snapshot" from "expired snapshot".
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone

from sequence_align.common import ident as _ident
from sequence_align.common import read as _read_strict_json

from .errors import (
    AlreadyInitializedError,
    CannotExpireHeadError,
    CorruptionError,
    LakeError,
    NotInitializedError,
    PinTokenError,
    SelectionError,
    SnapshotExpiredError,
    SnapshotNotFoundError,
    StaleSnapshotError,
)
from .schema import SCHEMA_SQL, SCHEMA_VERSION

CATALOG_NAME = "catalog.db"
DATA_DIR = "files/data"
DELETE_DIR = "files/deletes"
TMP_DIR = "files/tmp"

DEFAULT_TARGET_ROWS = 1000

# Test hooks (environment variables).  When set to a file path, the process
# creates that marker file at the named point and then sleeps forever, so a
# test can SIGKILL it at an exact crash point.  Never set in production.
_FREEZE_ENV = {
    "after_files": "LAKE_COMPACTOR_FREEZE_AFTER_FILES",
    "after_commit": "LAKE_COMPACTOR_FREEZE_AFTER_COMMIT",
    "after_gc_delete": "LAKE_COMPACTOR_FREEZE_AFTER_GC_DELETE",
}
# When set to a float, scan sleeps this many seconds before reading each data
# file after the first (used to interleave a gc with a paused scan in tests).
_SCAN_DELAY_ENV = "LAKE_COMPACTOR_SCAN_FILE_DELAY"


def _freeze_point(name: str) -> None:
    marker = os.environ.get(_FREEZE_ENV[name])
    if not marker:
        return
    with open(marker, "w", encoding="utf-8") as handle:
        handle.write(name)
    while True:  # pragma: no cover - the test kills us here
        time.sleep(3600)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _canonical(value) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError(f"nonfinite JSON number: {value}")


def _check_finite(value, ctx: str) -> None:
    """Reject non-finite floats anywhere inside a parsed JSON value.

    ``parse_constant`` catches the NaN/Infinity literals, but overflows such
    as ``1e999`` parse to ``inf`` silently, so walk the value explicitly.
    """
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"nonfinite JSON number in {ctx}")
    elif isinstance(value, list):
        for item in value:
            _check_finite(item, ctx)
    elif isinstance(value, dict):
        for item in value.values():
            _check_finite(item, ctx)


def _parse_jsonl_line(raw: bytes, ctx: str):
    """Parse one JSONL line strictly: UTF-8, one object, no duplicate keys,
    no non-finite numbers anywhere."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{ctx}: line is not valid UTF-8 ({exc})") from exc
    try:
        obj = json.loads(
            text, object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
        )
    except ValueError as exc:
        raise ValueError(f"{ctx}: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"{ctx}: line must be a JSON object")
    _check_finite(obj, ctx)
    return obj


def _require_fields(obj: dict, fields: str, ctx: str) -> None:
    wanted = set(fields.split())
    if not isinstance(obj, dict) or set(obj) != wanted:
        raise ValueError(f"{ctx}: expected object fields exactly: {fields}")


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _is_within(path: str, directory: str) -> bool:
    try:
        return os.path.commonpath([path, directory]) == directory
    except ValueError:
        return False


def _row_sort_key(row: dict):
    """The one deterministic row order used by compaction output and scan."""
    return (
        row["partition"], row["key"], row["data_seq"],
        row["origin_file_id"], row["origin_line"],
    )


def _delete_hits(delete: dict, row: dict) -> bool:
    """A delete predicate applies to rows of the same partition and key whose
    data_seq is not newer than the cutoff."""
    return (
        delete["partition"] == row["partition"]
        and delete["key"] == row["key"]
        and row["data_seq"] <= delete["cutoff"]
    )


def _parse_manifest(data) -> tuple[list, list]:
    """Structural validation of an append manifest (semantic checks such as
    the delete cutoff upper bound happen inside the transaction, where the
    new append sequence is known)."""
    _require_fields(data, "data_files deletes", "manifest")
    files = data["data_files"]
    deletes = data["deletes"]
    if not isinstance(files, list):
        raise ValueError("manifest data_files must be an array")
    if not isinstance(deletes, list):
        raise ValueError("manifest deletes must be an array")
    parsed_files = []
    for index, entry in enumerate(files):
        ctx = f"manifest data_files[{index}]"
        _require_fields(entry, "partition path", ctx)
        partition = entry["partition"]
        try:
            _ident(partition)
        except ValueError as exc:
            raise ValueError(f"{ctx}: invalid partition: {exc}") from exc
        path = entry["path"]
        if not isinstance(path, str) or not path:
            raise ValueError(f"{ctx}: path must be a non-empty string")
        if os.path.isabs(path) or "\0" in path:
            raise ValueError(f"{ctx}: path must be relative to the manifest")
        parsed_files.append({"partition": partition, "path": path})
    parsed_deletes = []
    for index, entry in enumerate(deletes):
        ctx = f"manifest deletes[{index}]"
        _require_fields(entry, "partition key cutoff", ctx)
        partition, key, cutoff = entry["partition"], entry["key"], entry["cutoff"]
        try:
            _ident(partition)
        except ValueError as exc:
            raise ValueError(f"{ctx}: invalid partition: {exc}") from exc
        try:
            _ident(key)
        except ValueError as exc:
            raise ValueError(f"{ctx}: invalid key: {exc}") from exc
        if not _is_int(cutoff) or cutoff < 0:
            raise ValueError(f"{ctx}: cutoff must be a non-negative integer")
        parsed_deletes.append(
            {"partition": partition, "key": key, "cutoff": cutoff}
        )
    return parsed_files, parsed_deletes


class Lake:
    """Owns one lake home directory (catalog + immutable JSONL files)."""

    # ------------------------------------------------------------- lifecycle
    def __init__(self, home: str):
        self.home = os.path.realpath(os.fspath(home))
        self._db_path = os.path.join(self.home, CATALOG_NAME)
        if not os.path.isfile(self._db_path):
            raise NotInitializedError(
                f"lake home {self.home!r} is not initialized; run init first"
            )
        self._lock = threading.RLock()
        # check_same_thread=False is safe: every use is guarded by _lock.
        self._conn = sqlite3.connect(
            self._db_path, isolation_level=None, check_same_thread=False,
            timeout=30,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        try:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
        except sqlite3.Error:
            self._conn.close()
            raise NotInitializedError(
                f"lake catalog at {self._db_path!r} is not a lake catalog"
            ) from None
        if row is None or int(row["value"]) != SCHEMA_VERSION:
            self._conn.close()
            raise NotInitializedError(
                f"lake catalog at {self._db_path!r} has an unsupported "
                f"schema version"
            )

    @classmethod
    def init(cls, home: str) -> dict:
        """Create a new lake home with the empty snapshot 0."""
        home_real = os.path.realpath(os.fspath(home))
        db_path = os.path.join(home_real, CATALOG_NAME)
        if os.path.exists(db_path):
            raise AlreadyInitializedError(
                f"lake home {home_real!r} is already initialized"
            )
        for sub in (DATA_DIR, DELETE_DIR, TMP_DIR):
            os.makedirs(os.path.join(home_real, sub), exist_ok=True)
        conn = sqlite3.connect(db_path, isolation_level=None)
        try:
            conn.executescript(SCHEMA_SQL)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                for key, value in (
                    ("head_snapshot", "0"),
                    ("append_seq", "0"),
                    ("next_file_id", "1"),
                    ("next_delete_file_id", "1"),
                    ("next_delete_id", "1"),
                ):
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES(?, ?)",
                        (key, value),
                    )
                conn.execute(
                    "INSERT INTO snapshots(snapshot_id, kind, append_seq, "
                    "expired, created_at) VALUES(0, 'init', NULL, 0, ?)",
                    (_utc_now(),),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return {
            "home": home_real,
            "snapshot_id": 0,
            "head": 0,
            "schema_version": SCHEMA_VERSION,
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "Lake":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- internals
    def _meta(self, key: str) -> int:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)
        ).fetchone()
        return int(row["value"])

    def _set_meta(self, key: str, value: int) -> None:
        self._conn.execute(
            "UPDATE meta SET value=? WHERE key=?", (str(value), key)
        )

    def _alloc_id(self, meta_key: str, prefix: str) -> str:
        """Allocate a never-reused id inside the open write transaction."""
        value = self._meta(meta_key)
        self._set_meta(meta_key, value + 1)
        return f"{prefix}{value:06d}"

    def _head(self) -> int:
        return self._meta("head_snapshot")

    def _check_head(self, expected_snapshot) -> int:
        if not _is_int(expected_snapshot) or expected_snapshot < 0:
            raise ValueError("expected_snapshot must be a non-negative integer")
        head = self._head()
        if expected_snapshot != head:
            raise StaleSnapshotError(
                f"stale expected_snapshot {expected_snapshot}: "
                f"current head is {head}"
            )
        return head

    def _snapshot_files(self, snapshot_id: int) -> list:
        return self._conn.execute(
            "SELECT df.* FROM snapshot_data_files sdf "
            "JOIN data_files df ON df.file_id = sdf.file_id "
            "WHERE sdf.snapshot_id=? ORDER BY df.file_id",
            (snapshot_id,),
        ).fetchall()

    def _snapshot_row(self, snapshot_id: int):
        return self._conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
        ).fetchone()

    # ------------------------------------------------------- file validation
    def _read_verified(self, file_id: str, relpath: str, sha256: str) -> bytes:
        """Read a tool-owned file and verify it against the catalog hash.

        Any deviation raises the stable CorruptionError instead of silently
        returning fewer or different rows.
        """
        path = os.path.join(self.home, relpath)
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except (FileNotFoundError, NotADirectoryError):
            raise CorruptionError(file_id, relpath, "file is missing") from None
        except IsADirectoryError:
            raise CorruptionError(
                file_id, relpath, "not a regular file"
            ) from None
        if hashlib.sha256(raw).hexdigest() != sha256:
            raise CorruptionError(file_id, relpath, "sha256 mismatch")
        return raw

    @staticmethod
    def _split_lines(raw: bytes) -> list:
        lines = raw.split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()  # tolerate a single trailing newline
        return lines

    def _read_data_rows(self, file_row) -> list:
        """Return all rows of a committed data file with their immutable
        identity (origin_file_id, origin_line, data_seq)."""
        file_id = file_row["file_id"]
        relpath = file_row["path"]
        raw = self._read_verified(file_id, relpath, file_row["sha256"])
        rows = []
        for lineno, raw_line in enumerate(self._split_lines(raw), 1):
            ctx = f"line {lineno}"
            try:
                obj = _parse_jsonl_line(raw_line, ctx)
            except ValueError as exc:
                raise CorruptionError(file_id, relpath, str(exc)) from None
            if file_row["kind"] == "import":
                # Verbatim copy of a source file: identity comes from the
                # catalog (file -> append seq) plus the line position.
                try:
                    _require_fields(obj, "key value", ctx)
                    _ident(obj["key"])
                except ValueError as exc:
                    raise CorruptionError(file_id, relpath, str(exc)) from None
                rows.append({
                    "partition": file_row["partition"],
                    "key": obj["key"],
                    "value": obj["value"],
                    "origin_file_id": file_id,
                    "origin_line": lineno,
                    "data_seq": file_row["data_seq"],
                })
            else:
                # Compacted file: self-describing lines carry the identity of
                # the original imported row.
                try:
                    _require_fields(
                        obj, "key value origin_file_id origin_line data_seq",
                        ctx,
                    )
                    _ident(obj["key"])
                    if not isinstance(obj["origin_file_id"], str) \
                            or not obj["origin_file_id"]:
                        raise ValueError(f"{ctx}: invalid origin_file_id")
                    if not _is_int(obj["origin_line"]) or obj["origin_line"] < 1:
                        raise ValueError(f"{ctx}: invalid origin_line")
                    if not _is_int(obj["data_seq"]) or obj["data_seq"] < 1:
                        raise ValueError(f"{ctx}: invalid data_seq")
                except ValueError as exc:
                    raise CorruptionError(file_id, relpath, str(exc)) from None
                rows.append({
                    "partition": file_row["partition"],
                    "key": obj["key"],
                    "value": obj["value"],
                    "origin_file_id": obj["origin_file_id"],
                    "origin_line": obj["origin_line"],
                    "data_seq": obj["data_seq"],
                })
        if len(rows) != file_row["row_count"]:
            raise CorruptionError(
                file_id, relpath,
                f"row count mismatch (catalog {file_row['row_count']}, "
                f"actual {len(rows)})",
            )
        return rows

    def _read_delete_entries(self, file_row) -> dict:
        """Return {line_number: delete predicate} for a committed delete file."""
        file_id = file_row["file_id"]
        relpath = file_row["path"]
        raw = self._read_verified(file_id, relpath, file_row["sha256"])
        entries = {}
        for lineno, raw_line in enumerate(self._split_lines(raw), 1):
            ctx = f"line {lineno}"
            try:
                obj = _parse_jsonl_line(raw_line, ctx)
                _require_fields(obj, "delete_id partition key cutoff", ctx)
                _ident(obj["partition"])
                _ident(obj["key"])
                if not isinstance(obj["delete_id"], str) \
                        or not obj["delete_id"]:
                    raise ValueError(f"{ctx}: invalid delete_id")
                if not _is_int(obj["cutoff"]) or obj["cutoff"] < 0:
                    raise ValueError(f"{ctx}: invalid cutoff")
            except ValueError as exc:
                raise CorruptionError(file_id, relpath, str(exc)) from None
            entries[lineno] = {
                "delete_id": obj["delete_id"],
                "partition": obj["partition"],
                "key": obj["key"],
                "cutoff": obj["cutoff"],
            }
        if len(entries) != file_row["entry_count"]:
            raise CorruptionError(
                file_id, relpath,
                f"entry count mismatch (catalog {file_row['entry_count']}, "
                f"actual {len(entries)})",
            )
        return entries

    def _load_snapshot_deletes(self, snapshot_id: int) -> list:
        """Resolve a snapshot's effective delete predicates from the
        tool-owned delete files (hash-verified)."""
        locations = self._conn.execute(
            "SELECT de.delete_id, de.file_id, de.line, df.path, df.sha256, "
            "df.entry_count FROM snapshot_deletes sd "
            "JOIN delete_entries de ON de.delete_id = sd.delete_id "
            "JOIN delete_files df ON df.file_id = de.file_id "
            "WHERE sd.snapshot_id=? ORDER BY de.delete_id",
            (snapshot_id,),
        ).fetchall()
        by_file: dict[str, dict] = {}
        for loc in locations:
            by_file.setdefault(loc["file_id"], {"row": loc, "lines": []})
            by_file[loc["file_id"]]["lines"].append(loc)
        deletes = []
        for file_id, group in by_file.items():
            row = group["row"]
            entries = self._read_delete_entries(
                {"file_id": file_id, "path": row["path"],
                 "sha256": row["sha256"], "entry_count": row["entry_count"]}
            )
            for loc in group["lines"]:
                entry = entries.get(loc["line"])
                if entry is None or entry["delete_id"] != loc["delete_id"]:
                    raise CorruptionError(
                        file_id, row["path"],
                        f"delete {loc['delete_id']} not found at "
                        f"line {loc['line']}",
                    )
                deletes.append(entry)
        return deletes

    # --------------------------------------------------------- file creation
    def _publish_file(self, tmp_path: str, final_rel: str) -> None:
        """Move a fully written tmp file into its immutable final location."""
        final_abs = os.path.join(self.home, final_rel)
        os.replace(tmp_path, final_abs)
        _fsync_dir(os.path.dirname(final_abs))

    def _copy_source_file(self, src: str, tmp_path: str, rel: str):
        """Copy a source JSONL file verbatim while validating every line.

        Returns (sha256, byte_count, row_count).  Any invalid line raises
        ValueError and the caller aborts the whole append.
        """
        digest = hashlib.sha256()
        rows = 0
        nbytes = 0
        with open(src, "rb") as fin, open(tmp_path, "wb") as fout:
            for raw_line in fin:
                rows += 1
                ctx = f"invalid JSONL line {rows} in {rel}"
                obj = _parse_jsonl_line(raw_line, ctx)
                _require_fields(obj, "key value", ctx)
                try:
                    _ident(obj["key"])
                except ValueError as exc:
                    raise ValueError(f"{ctx}: invalid key: {exc}") from exc
                fout.write(raw_line)
                digest.update(raw_line)
                nbytes += len(raw_line)
            fout.flush()
            os.fsync(fout.fileno())
        return digest.hexdigest(), nbytes, rows

    def _write_generated_file(self, tmp_path: str, lines: list):
        """Write a tool-generated JSONL file (canonical lines) and return
        (sha256, byte_count)."""
        digest = hashlib.sha256()
        nbytes = 0
        with open(tmp_path, "wb") as fout:
            for line in lines:
                raw = line.encode("utf-8") + b"\n"
                fout.write(raw)
                digest.update(raw)
                nbytes += len(raw)
            fout.flush()
            os.fsync(fout.fileno())
        return digest.hexdigest(), nbytes

    def _tmp_path(self, file_id: str) -> str:
        return os.path.join(
            self.home, TMP_DIR, f"{file_id}.{os.getpid()}.part"
        )

    # ================================================================= append
    def append(self, manifest_path, expected_snapshot, report_sink=None) -> dict:
        """Import one manifest of data files plus deletes as a new snapshot.

        The whole append -- file copies, delete file, catalog rows, head
        switch -- commits atomically; any invalid JSONL line fails the entire
        append and consumes no append sequence.
        """
        manifest_path = os.fspath(manifest_path)
        if os.path.islink(manifest_path):
            raise ValueError("manifest cannot be a symlink")
        data, manifest_sha = _read_strict_json(manifest_path)
        files_spec, deletes_spec = _parse_manifest(data)
        base = os.path.dirname(os.path.abspath(manifest_path))

        # Resolve and check every source path before opening the transaction.
        sources = []
        for spec in files_spec:
            rel = spec["path"]
            joined = os.path.join(base, rel)
            if os.path.islink(joined):
                raise ValueError(f"data file is a symlink: {rel}")
            real = os.path.realpath(joined)
            if _is_within(real, self.home):
                raise ValueError(
                    f"source path conflicts with the lake home: {rel}"
                )
            if not os.path.isfile(real):
                raise ValueError(f"data file not found: {rel}")
            sources.append(real)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            created: list[str] = []  # final paths to unlink best-effort on failure
            tmps: list[str] = []
            try:
                head = self._check_head(expected_snapshot)
                append_seq = self._meta("append_seq") + 1
                for delete in deletes_spec:
                    if delete["cutoff"] > append_seq:
                        raise ValueError(
                            f"delete cutoff {delete['cutoff']} exceeds this "
                            f"append's sequence {append_seq}"
                        )

                new_files = []
                for spec, src in zip(files_spec, sources):
                    file_id = self._alloc_id("next_file_id", "d")
                    final_rel = f"{DATA_DIR}/{file_id}.jsonl"
                    tmp = self._tmp_path(file_id)
                    tmps.append(tmp)
                    sha256, nbytes, rows = self._copy_source_file(
                        src, tmp, spec["path"]
                    )
                    self._publish_file(tmp, final_rel)
                    tmps.remove(tmp)
                    created.append(os.path.join(self.home, final_rel))
                    self._conn.execute(
                        "INSERT INTO data_files(file_id, partition, kind, "
                        "data_seq, path, bytes, sha256, row_count) "
                        "VALUES(?, ?, 'import', ?, ?, ?, ?, ?)",
                        (file_id, spec["partition"], append_seq, final_rel,
                         nbytes, sha256, rows),
                    )
                    new_files.append({
                        "file_id": file_id,
                        "partition": spec["partition"],
                        "source": spec["path"],
                        "rows": rows,
                        "bytes": nbytes,
                        "sha256": sha256,
                    })

                # The append's delete set becomes its own immutable JSONL file
                # with a stable delete_id per entry (even when empty).
                delete_file_id = self._alloc_id("next_delete_file_id", "x")
                delete_lines = []
                delete_ids = []
                for delete in deletes_spec:
                    delete_id = self._alloc_id("next_delete_id", "g")
                    delete_ids.append(delete_id)
                    delete_lines.append(_canonical({
                        "delete_id": delete_id,
                        "partition": delete["partition"],
                        "key": delete["key"],
                        "cutoff": delete["cutoff"],
                    }))
                del_rel = f"{DELETE_DIR}/{delete_file_id}.jsonl"
                tmp = self._tmp_path(delete_file_id)
                tmps.append(tmp)
                del_sha, del_bytes = self._write_generated_file(
                    tmp, delete_lines
                )
                self._publish_file(tmp, del_rel)
                tmps.remove(tmp)
                created.append(os.path.join(self.home, del_rel))
                self._conn.execute(
                    "INSERT INTO delete_files(file_id, path, bytes, sha256, "
                    "entry_count, append_seq) VALUES(?, ?, ?, ?, ?, ?)",
                    (delete_file_id, del_rel, del_bytes, del_sha,
                     len(delete_lines), append_seq),
                )
                for line_no, delete_id in enumerate(delete_ids, 1):
                    self._conn.execute(
                        "INSERT INTO delete_entries(delete_id, file_id, line) "
                        "VALUES(?, ?, ?)",
                        (delete_id, delete_file_id, line_no),
                    )

                new_snapshot = head + 1
                self._conn.execute(
                    "INSERT INTO snapshots(snapshot_id, kind, append_seq, "
                    "expired, created_at) VALUES(?, 'append', ?, 0, ?)",
                    (new_snapshot, append_seq, _utc_now()),
                )
                self._conn.execute(
                    "INSERT INTO snapshot_data_files(snapshot_id, file_id) "
                    "SELECT ?, file_id FROM snapshot_data_files "
                    "WHERE snapshot_id=?",
                    (new_snapshot, head),
                )
                for entry in new_files:
                    self._conn.execute(
                        "INSERT INTO snapshot_data_files(snapshot_id, file_id)"
                        " VALUES(?, ?)",
                        (new_snapshot, entry["file_id"]),
                    )
                self._conn.execute(
                    "INSERT INTO snapshot_deletes(snapshot_id, delete_id) "
                    "SELECT ?, delete_id FROM snapshot_deletes "
                    "WHERE snapshot_id=?",
                    (new_snapshot, head),
                )
                for delete_id in delete_ids:
                    self._conn.execute(
                        "INSERT INTO snapshot_deletes(snapshot_id, delete_id)"
                        " VALUES(?, ?)",
                        (new_snapshot, delete_id),
                    )
                self._set_meta("head_snapshot", new_snapshot)
                self._set_meta("append_seq", append_seq)

                report = {
                    "snapshot_id": new_snapshot,
                    "expected_snapshot": expected_snapshot,
                    "head": new_snapshot,
                    "append_seq": append_seq,
                    "manifest_sha256": manifest_sha,
                    "data_files": new_files,
                    "delete_file": {
                        "file_id": delete_file_id,
                        "entries": len(delete_lines),
                        "bytes": del_bytes,
                        "sha256": del_sha,
                    },
                    "deletes": len(delete_lines),
                }
                if report_sink is not None:
                    report_sink(report)
                _freeze_point("after_files")
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                self._cleanup_uncommitted(tmps + created)
                raise
        # Committed: the files above are now published and must never be
        # cleaned by the failure path, so the post-commit hook runs here.
        _freeze_point("after_commit")
        return report

    def _cleanup_uncommitted(self, paths) -> None:
        """Best-effort unlink of files written by a rolled-back mutation.

        These paths were created by this very transaction and were never
        committed to the catalog, so removing them is always safe; ``gc`` is
        the backstop for anything missed (e.g. after a hard kill).
        """
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass

    # ================================================================ compact
    def compact(self, partition, file_ids, expected_snapshot,
                target_rows=DEFAULT_TARGET_ROWS, report_sink=None) -> dict:
        """Rewrite explicitly selected current data files of one partition.

        Only the selected files are rewritten; other partitions and
        unselected files are never touched.  Surviving rows keep their
        original identity and data_seq; the new snapshot keeps exactly the
        delete predicates that still hit a stored row of an unselected file.
        """
        try:
            _ident(partition)
        except ValueError as exc:
            raise ValueError(f"invalid partition: {exc}") from exc
        if not isinstance(file_ids, (list, tuple)):
            raise SelectionError("files must be a list of file ids")
        file_ids = list(file_ids)
        if len(file_ids) < 2:
            raise SelectionError(
                "compaction requires at least two selected data files"
            )
        if not all(isinstance(f, str) and f for f in file_ids):
            raise SelectionError("files must be non-empty file id strings")
        if len(set(file_ids)) != len(file_ids):
            raise SelectionError("duplicate file ids in selection")
        if not _is_int(target_rows) or target_rows < 1:
            raise ValueError("target_rows must be a positive integer")

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            created: list[str] = []
            tmps: list[str] = []
            try:
                head = self._check_head(expected_snapshot)
                head_files = self._snapshot_files(head)
                by_id = {row["file_id"]: row for row in head_files}
                for file_id in file_ids:
                    if file_id not in by_id:
                        raise SelectionError(
                            f"{file_id} is not a current data file"
                        )
                    owner = by_id[file_id]["partition"]
                    if owner != partition:
                        raise SelectionError(
                            f"{file_id} belongs to partition {owner!r}, "
                            f"not {partition!r}"
                        )
                selected_set = set(file_ids)
                selected = [by_id[f] for f in sorted(selected_set)]
                unselected = [
                    row for row in head_files
                    if row["file_id"] not in selected_set
                ]

                deletes = self._load_snapshot_deletes(head)

                # Rewrite only the selected files: keep rows not deleted by
                # the current snapshot, with identity and data_seq intact.
                input_rows = 0
                survivors = []
                for file_row in selected:
                    for row in self._read_data_rows(file_row):
                        input_rows += 1
                        if not any(_delete_hits(d, row) for d in deletes):
                            survivors.append(row)
                survivors.sort(key=_row_sort_key)

                outputs = []
                for start in range(0, len(survivors), target_rows):
                    chunk = survivors[start:start + target_rows]
                    file_id = self._alloc_id("next_file_id", "d")
                    final_rel = f"{DATA_DIR}/{file_id}.jsonl"
                    lines = [
                        _canonical({
                            "key": row["key"],
                            "value": row["value"],
                            "origin_file_id": row["origin_file_id"],
                            "origin_line": row["origin_line"],
                            "data_seq": row["data_seq"],
                        })
                        for row in chunk
                    ]
                    tmp = self._tmp_path(file_id)
                    tmps.append(tmp)
                    sha256, nbytes = self._write_generated_file(tmp, lines)
                    self._publish_file(tmp, final_rel)
                    tmps.remove(tmp)
                    created.append(os.path.join(self.home, final_rel))
                    self._conn.execute(
                        "INSERT INTO data_files(file_id, partition, kind, "
                        "data_seq, path, bytes, sha256, row_count) "
                        "VALUES(?, ?, 'compact', NULL, ?, ?, ?, ?)",
                        (file_id, partition, final_rel, nbytes, sha256,
                         len(chunk)),
                    )
                    outputs.append({
                        "file_id": file_id,
                        "rows": len(chunk),
                        "bytes": nbytes,
                        "sha256": sha256,
                    })

                # Delete survival: a predicate stays in the new snapshot iff
                # it still hits a stored row of an *unselected* file.  Hits
                # are evaluated against stored rows (not delete-filtered
                # ones); the minimum data_seq per (partition, key) decides.
                min_seq: dict[tuple, int] = {}
                for file_row in unselected:
                    for row in self._read_data_rows(file_row):
                        tag = (row["partition"], row["key"])
                        if tag not in min_seq or row["data_seq"] < min_seq[tag]:
                            min_seq[tag] = row["data_seq"]
                kept = [
                    d for d in deletes
                    if min_seq.get((d["partition"], d["key"])) is not None
                    and min_seq[(d["partition"], d["key"])] <= d["cutoff"]
                ]

                new_snapshot = head + 1
                self._conn.execute(
                    "INSERT INTO snapshots(snapshot_id, kind, append_seq, "
                    "expired, created_at) VALUES(?, 'compact', NULL, 0, ?)",
                    (new_snapshot, _utc_now()),
                )
                for row in unselected:
                    self._conn.execute(
                        "INSERT INTO snapshot_data_files(snapshot_id, file_id)"
                        " VALUES(?, ?)",
                        (new_snapshot, row["file_id"]),
                    )
                for entry in outputs:
                    self._conn.execute(
                        "INSERT INTO snapshot_data_files(snapshot_id, file_id)"
                        " VALUES(?, ?)",
                        (new_snapshot, entry["file_id"]),
                    )
                for delete in kept:
                    self._conn.execute(
                        "INSERT INTO snapshot_deletes(snapshot_id, delete_id)"
                        " VALUES(?, ?)",
                        (new_snapshot, delete["delete_id"]),
                    )
                self._set_meta("head_snapshot", new_snapshot)

                report = {
                    "snapshot_id": new_snapshot,
                    "expected_snapshot": expected_snapshot,
                    "head": new_snapshot,
                    "partition": partition,
                    "inputs": sorted(selected_set),
                    "outputs": outputs,
                    "input_rows": input_rows,
                    "output_rows": len(survivors),
                    "deleted_rows": input_rows - len(survivors),
                    "deletes_before": len(deletes),
                    "deletes_kept": len(kept),
                    "deletes_dropped": len(deletes) - len(kept),
                }
                if report_sink is not None:
                    report_sink(report)
                _freeze_point("after_files")
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                self._cleanup_uncommitted(tmps + created)
                raise
        # Committed: the outputs are now published (see append).
        _freeze_point("after_commit")
        return report

    # =================================================================== pin
    def _mutate(self, body, report_sink=None):
        """Run a small catalog-only mutation in one immediate transaction."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                report = body()
                if report_sink is not None:
                    report_sink(report)
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return report

    def pin(self, snapshot_id, report_sink=None) -> dict:
        """Pin a not-yet-expired snapshot and return a persistent token."""
        if not _is_int(snapshot_id) or snapshot_id < 0:
            raise ValueError("snapshot_id must be a non-negative integer")

        def body():
            row = self._snapshot_row(snapshot_id)
            if row is None:
                raise SnapshotNotFoundError(f"unknown snapshot {snapshot_id}")
            if row["expired"]:
                raise SnapshotExpiredError(
                    f"snapshot {snapshot_id} is expired"
                )
            token = "pin-" + secrets.token_hex(16)
            self._conn.execute(
                "INSERT INTO pins(token, snapshot_id, created_at) "
                "VALUES(?, ?, ?)",
                (token, snapshot_id, _utc_now()),
            )
            return {"token": token, "snapshot_id": snapshot_id}

        return self._mutate(body, report_sink)

    def unpin(self, token, report_sink=None) -> dict:
        """Release a pin; the token is invalid afterwards."""
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")

        def body():
            row = self._conn.execute(
                "SELECT * FROM pins WHERE token=?", (token,)
            ).fetchone()
            if row is None:
                raise PinTokenError("unknown pin token")
            self._conn.execute(
                "DELETE FROM pins WHERE token=?", (token,)
            )
            return {
                "token": token,
                "snapshot_id": row["snapshot_id"],
                "unpinned": True,
            }

        return self._mutate(body, report_sink)

    def expire(self, snapshot_id, report_sink=None) -> dict:
        """Make a non-current snapshot un-pinnable.  Existing pins keep
        reading; there is no automatic lease."""
        if not _is_int(snapshot_id) or snapshot_id < 0:
            raise ValueError("snapshot_id must be a non-negative integer")

        def body():
            row = self._snapshot_row(snapshot_id)
            if row is None:
                raise SnapshotNotFoundError(f"unknown snapshot {snapshot_id}")
            if snapshot_id == self._head():
                raise CannotExpireHeadError(
                    f"cannot expire the current head snapshot {snapshot_id}"
                )
            if row["expired"]:
                raise SnapshotExpiredError(
                    f"snapshot {snapshot_id} is already expired"
                )
            self._conn.execute(
                "UPDATE snapshots SET expired=1 WHERE snapshot_id=?",
                (snapshot_id,),
            )
            return {"snapshot_id": snapshot_id, "expired": True}

        return self._mutate(body, report_sink)

    # ================================================================== scan
    def scan(self, token) -> dict:
        """Return the visible rows (with original identity) of the snapshot
        held by a valid pin token.

        The token is validated and the file list resolved inside one
        read-only catalog snapshot; the content files of a pinned snapshot
        are protected from gc until unpin, so the reads below cannot lose a
        file even if a gc commits while the scan is paused between files.
        """
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")
        with self._lock:
            pin = self._conn.execute(
                "SELECT * FROM pins WHERE token=?", (token,)
            ).fetchone()
            if pin is None:
                raise PinTokenError("unknown pin token")
            snapshot_id = pin["snapshot_id"]
            files = self._snapshot_files(snapshot_id)
            deletes = self._load_snapshot_deletes(snapshot_id)

        delay = float(os.environ.get(_SCAN_DELAY_ENV, "0") or 0)
        rows = []
        for index, file_row in enumerate(files):
            if index > 0 and delay > 0:  # pragma: no cover - test hook
                time.sleep(delay)
            rows.extend(self._read_data_rows(file_row))
        visible = [
            row for row in rows
            if not any(_delete_hits(d, row) for d in deletes)
        ]
        visible.sort(key=_row_sort_key)
        return {
            "token": token,
            "snapshot_id": snapshot_id,
            "row_count": len(visible),
            "rows": visible,
        }

    # ==================================================================== gc
    def gc(self, report_sink=None) -> dict:
        """Delete every tool-owned file not reachable from the current head,
        an unexpired snapshot, or a valid pin -- and nothing else.

        Runs inside the write transaction, so no live writer can be
        mid-publication: any on-disk file absent from the catalog belongs to
        a dead writer and is an orphan.  File deletion is idempotent and the
        catalog prune is re-runnable, so a crash anywhere leaves a state the
        next gc completes.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                head = self._head()
                protected = {head}
                protected.update(
                    row["snapshot_id"] for row in self._conn.execute(
                        "SELECT snapshot_id FROM snapshots WHERE expired=0"
                    )
                )
                protected.update(
                    row["snapshot_id"] for row in self._conn.execute(
                        "SELECT DISTINCT snapshot_id FROM pins"
                    )
                )
                marks = ",".join(str(s) for s in sorted(protected))
                reachable = {
                    row["path"] for row in self._conn.execute(
                        "SELECT DISTINCT df.path FROM snapshot_data_files sdf "
                        "JOIN data_files df ON df.file_id = sdf.file_id "
                        f"WHERE sdf.snapshot_id IN ({marks})"
                    )
                }
                reachable.update(
                    row["path"] for row in self._conn.execute(
                        "SELECT DISTINCT df.path FROM snapshot_deletes sd "
                        "JOIN delete_entries de "
                        "  ON de.delete_id = sd.delete_id "
                        "JOIN delete_files df ON df.file_id = de.file_id "
                        f"WHERE sd.snapshot_id IN ({marks})"
                    )
                )

                deleted = []
                for sub in (DATA_DIR, DELETE_DIR, TMP_DIR):
                    directory = os.path.join(self.home, sub)
                    if not os.path.isdir(directory):
                        continue
                    for name in sorted(os.listdir(directory)):
                        full = os.path.join(directory, name)
                        if not os.path.isfile(full):
                            continue
                        rel = f"{sub}/{name}"
                        if rel in reachable:
                            continue
                        size = os.path.getsize(full)
                        os.unlink(full)
                        deleted.append({"path": rel, "bytes": size})

                # Prune catalog rows that are now unreachable.  Snapshot
                # header rows stay (history + stable pin errors); only
                # associations and unreferenced file/entry rows go away.
                self._conn.execute(
                    f"DELETE FROM snapshot_data_files "
                    f"WHERE snapshot_id NOT IN ({marks})"
                )
                self._conn.execute(
                    f"DELETE FROM snapshot_deletes "
                    f"WHERE snapshot_id NOT IN ({marks})"
                )
                self._conn.execute(
                    "DELETE FROM data_files WHERE file_id NOT IN "
                    "(SELECT file_id FROM snapshot_data_files)"
                )
                self._conn.execute(
                    "DELETE FROM delete_entries WHERE delete_id NOT IN "
                    "(SELECT delete_id FROM snapshot_deletes)"
                )
                self._conn.execute(
                    "DELETE FROM delete_files WHERE file_id NOT IN "
                    "(SELECT file_id FROM delete_entries)"
                )

                report = {
                    "head": head,
                    "protected_snapshots": sorted(protected),
                    "deleted_files": deleted,
                    "deleted_count": len(deleted),
                    "freed_bytes": sum(item["bytes"] for item in deleted),
                }
                if report_sink is not None:
                    report_sink(report)
                _freeze_point("after_gc_delete")
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return report

    # ============================================================ inspection
    def head_snapshot(self) -> int:
        """The current head snapshot id."""
        with self._lock:
            return self._head()

    def append_seq(self) -> int:
        """The latest allocated append (data) sequence."""
        with self._lock:
            return self._meta("append_seq")

    def snapshot_info(self, snapshot_id: int) -> dict:
        """Read-only view of one snapshot's dependencies (catalog metadata;
        delete predicates are re-read from the hash-verified delete files)."""
        if not _is_int(snapshot_id) or snapshot_id < 0:
            raise ValueError("snapshot_id must be a non-negative integer")
        with self._lock:
            row = self._snapshot_row(snapshot_id)
            if row is None:
                raise SnapshotNotFoundError(f"unknown snapshot {snapshot_id}")
            files = self._snapshot_files(snapshot_id)
            deletes = self._load_snapshot_deletes(snapshot_id)
            return {
                "snapshot_id": snapshot_id,
                "kind": row["kind"],
                "append_seq": row["append_seq"],
                "expired": bool(row["expired"]),
                "is_head": snapshot_id == self._head(),
                "data_files": [
                    {
                        "file_id": f["file_id"],
                        "partition": f["partition"],
                        "kind": f["kind"],
                        "data_seq": f["data_seq"],
                        "rows": f["row_count"],
                        "bytes": f["bytes"],
                        "sha256": f["sha256"],
                        "path": f["path"],
                    }
                    for f in files
                ],
                "deletes": deletes,
            }
