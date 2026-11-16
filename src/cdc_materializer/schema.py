"""SQLite schema for the CDC history materializer.

Two time dimensions are stored explicitly:

* business valid time is each fact's ``valid_from``; intervals are half-open
  because a point-in-time query selects the greatest ``valid_from <= as_of``,
  so a later start takes over at its own inclusive boundary;
* knowledge time is the gap ``[create_seq, close_seq)`` on every facts row,
  i.e. the range of knowledge sequence numbers under which the row is part of
  the materialized view.

Immutable, accepted history is kept in ``events``; ``facts`` is derived from
it and only ever gets rows appended or closed (never rewritten), so queries at
an earlier ``known_through`` keep returning the same facts forever.
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

-- One row per ingested batch.  raw_hash makes duplicate delivery idempotent:
-- the same batch_id must always carry byte-identical canonical contents.
CREATE TABLE IF NOT EXISTS batches(
    batch_id        TEXT PRIMARY KEY,
    raw_hash        TEXT NOT NULL,
    record_count    INTEGER NOT NULL,
    accepted_count  INTEGER NOT NULL,
    rejected_count  INTEGER NOT NULL,
    knowledge_seq   INTEGER,              -- NULL when nothing was accepted
    ingested_at     TEXT NOT NULL
);

-- Accepted events are immutable append-only history.
CREATE TABLE IF NOT EXISTS events(
    record_index   INTEGER NOT NULL,
    event_id      TEXT PRIMARY KEY,
    source        TEXT NOT NULL,
    entity        TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    op            TEXT NOT NULL CHECK(op IN ('upsert','retract')),
    valid_time    TEXT NOT NULL,
    payload_json  TEXT,
    target_event_id TEXT,
    knowledge_seq INTEGER NOT NULL,
    batch_id      TEXT NOT NULL,
    ingested_at   TEXT NOT NULL,
    UNIQUE(source, entity, revision)
);

-- Per-record rejection ledger, keyed by event_id when the record carried one.
-- Structurally broken records may have NULL columns and an empty event_id.
CREATE TABLE IF NOT EXISTS rejections(
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      TEXT NOT NULL,
    record_index  INTEGER NOT NULL,
    event_id      TEXT,
    source        TEXT,
    entity        TEXT,
    revision      INTEGER,
    op            TEXT,
    valid_time    TEXT,
    reason_code   TEXT NOT NULL,
    reason_detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejections_event
    ON rejections(event_id);
CREATE INDEX IF NOT EXISTS idx_rejections_batch
    ON rejections(batch_id, record_index);

-- Bi-temporal fact rows.  Only upsert facts live here; a retraction simply
-- closes the target's rows (append-only, never rewritten).  Each row says:
-- "value event_id was the materialized value from valid_from onwards, for
-- knowledge sequence numbers in [create_seq, close_seq)".  Point-in-time
-- queries pick the row with the greatest valid_from <= as_of whose knowledge
-- interval still contains known_through; later valid starts naturally take
-- over at their own (inclusive) boundary, which keeps valid time half-open
-- without storing an explicit end.
CREATE TABLE IF NOT EXISTS facts(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    entity          TEXT NOT NULL,
    valid_from      TEXT NOT NULL,
    create_seq      INTEGER NOT NULL,
    close_seq       INTEGER,
    value_json      TEXT NOT NULL,
    event_id        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_facts_lookup
    ON facts(source, entity, create_seq, close_seq, valid_from);
CREATE INDEX IF NOT EXISTS idx_facts_start
    ON facts(source, entity, valid_from, create_seq, close_seq);

-- Every knowledge sequence that touched an entity gets a row, so a query at
-- any cutoff can find the entity's then-current head without scanning facts.
CREATE TABLE IF NOT EXISTS entity_versions(
    source     TEXT NOT NULL,
    entity     TEXT NOT NULL,
    head_seq   INTEGER NOT NULL,
    PRIMARY KEY(source, entity, head_seq)
);
"""
