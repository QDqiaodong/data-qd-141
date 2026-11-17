"""SQLite catalog schema for the lake compactor.

The catalog is *metadata only*: it tracks files, snapshot memberships, delete
entry locations and pins.  Row content (keys and values) lives exclusively in
the immutable JSONL files under ``files/`` -- no table ever stores a value, so
the catalog can never silently replace the lake files.

* ``snapshots`` rows are never deleted: a snapshot id stays meaningful (and
  its expired state queryable) forever.  Only the *associations* of snapshots
  that are neither current, unexpired nor pinned are pruned by ``gc``.
* ``data_files.data_seq`` is the append sequence for ``kind='import'`` files
  (every row in them shares it).  Compacted files (``kind='compact'``) carry
  per-row ``data_seq`` inside their JSONL lines, so the column is NULL there.
* ``delete_entries`` maps a stable delete id to its storage location
  (delete file + 1-based line).  The predicate itself
  (partition/key/cutoff) is stored only inside the immutable delete file, so
  delete files are genuine dependencies that ``gc`` must preserve.
"""

SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per snapshot, kept forever.  kind is init/append/compact;
-- append_seq is set exactly for append snapshots (the data_seq it allocated).
CREATE TABLE IF NOT EXISTS snapshots(
    snapshot_id INTEGER PRIMARY KEY,
    kind        TEXT NOT NULL CHECK(kind IN ('init','append','compact')),
    append_seq  INTEGER,
    expired     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
);

-- Immutable tool-owned data files.  path is relative to the lake home.
CREATE TABLE IF NOT EXISTS data_files(
    file_id   TEXT PRIMARY KEY,
    partition TEXT NOT NULL,
    kind      TEXT NOT NULL CHECK(kind IN ('import','compact')),
    data_seq  INTEGER,
    path      TEXT NOT NULL,
    bytes     INTEGER NOT NULL,
    sha256    TEXT NOT NULL,
    row_count INTEGER NOT NULL
);

-- Immutable tool-owned delete files (one per append).
CREATE TABLE IF NOT EXISTS delete_files(
    file_id     TEXT PRIMARY KEY,
    path        TEXT NOT NULL,
    bytes       INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    entry_count INTEGER NOT NULL,
    append_seq  INTEGER NOT NULL
);

-- Stable delete identity -> storage location.  The predicate itself lives
-- only inside the delete file (see module docstring).
CREATE TABLE IF NOT EXISTS delete_entries(
    delete_id TEXT PRIMARY KEY,
    file_id   TEXT NOT NULL,
    line      INTEGER NOT NULL
);

-- Snapshot memberships: which data files / delete entries are effective.
CREATE TABLE IF NOT EXISTS snapshot_data_files(
    snapshot_id INTEGER NOT NULL,
    file_id     TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_sdf_file
    ON snapshot_data_files(file_id);

CREATE TABLE IF NOT EXISTS snapshot_deletes(
    snapshot_id INTEGER NOT NULL,
    delete_id   TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, delete_id)
);
CREATE INDEX IF NOT EXISTS idx_sd_delete
    ON snapshot_deletes(delete_id);

-- Persistent pins: a token keeps its snapshot's dependencies alive until
-- explicitly unpinned.  No leases, no expiry fields.
CREATE TABLE IF NOT EXISTS pins(
    token       TEXT PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);
"""
