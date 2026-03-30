CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT,
    summary TEXT,
    tags_json TEXT,
    metadata_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE(entity_type, slug),
    CHECK(tags_json IS NULL OR json_valid(tags_json)),
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    artifact_type TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    source_path TEXT,
    mime_type TEXT,
    observed_at_utc TEXT NOT NULL,
    entity_id TEXT,
    metadata_json TEXT,
    UNIQUE(sha256, storage_path),
    FOREIGN KEY(entity_id) REFERENCES entities(id),
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE TABLE IF NOT EXISTS relations (
    id TEXT PRIMARY KEY,
    from_entity_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    to_entity_id TEXT NOT NULL,
    rationale TEXT,
    metadata_json TEXT,
    created_at_utc TEXT NOT NULL,
    UNIQUE(from_entity_id, relation_type, to_entity_id),
    FOREIGN KEY(from_entity_id) REFERENCES entities(id),
    FOREIGN KEY(to_entity_id) REFERENCES entities(id),
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    occurred_at_utc TEXT NOT NULL,
    occurred_at_local TEXT,
    timezone TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    actor TEXT,
    event_type TEXT NOT NULL,
    category TEXT,
    entity_type TEXT,
    entity_id TEXT,
    title TEXT,
    text TEXT NOT NULL,
    why TEXT,
    circumstances TEXT,
    source_kind TEXT,
    source_path TEXT,
    source_hash TEXT,
    payload_json TEXT,
    imported_from TEXT,
    mem0_status TEXT,
    mem0_error TEXT,
    CHECK(payload_json IS NULL OR json_valid(payload_json)),
    FOREIGN KEY(entity_id) REFERENCES entities(id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_events_occurred_at_utc ON events(occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id, occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_events_category ON events(category, occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, occurred_at_utc);

CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    event_id UNINDEXED,
    title,
    text,
    why,
    circumstances,
    tokenize = 'unicode61'
);

CREATE TRIGGER IF NOT EXISTS events_ai AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, event_id, title, text, why, circumstances)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        new.text,
        COALESCE(new.why, ''),
        COALESCE(new.circumstances, '')
    );
END;

CREATE TRIGGER IF NOT EXISTS events_ad AFTER DELETE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, event_id, title, text, why, circumstances)
    VALUES (
        'delete',
        old.rowid,
        old.id,
        COALESCE(old.title, ''),
        old.text,
        COALESCE(old.why, ''),
        COALESCE(old.circumstances, '')
    );
END;

CREATE TRIGGER IF NOT EXISTS events_au AFTER UPDATE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, event_id, title, text, why, circumstances)
    VALUES (
        'delete',
        old.rowid,
        old.id,
        COALESCE(old.title, ''),
        old.text,
        COALESCE(old.why, ''),
        COALESCE(old.circumstances, '')
    );
    INSERT INTO events_fts(rowid, event_id, title, text, why, circumstances)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        new.text,
        COALESCE(new.why, ''),
        COALESCE(new.circumstances, '')
    );
END;

CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    captured_at_utc TEXT NOT NULL,
    captured_at_local TEXT NOT NULL,
    timezone TEXT NOT NULL,
    agent TEXT,
    domain TEXT,
    title TEXT,
    focus TEXT,
    label TEXT,
    summary_text TEXT,
    payload_json TEXT NOT NULL,
    CHECK(json_valid(payload_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at_utc ON snapshots(captured_at_utc);
CREATE INDEX IF NOT EXISTS idx_snapshots_domain ON snapshots(domain, captured_at_utc);

CREATE VIRTUAL TABLE IF NOT EXISTS snapshots_fts USING fts5(
    snapshot_id UNINDEXED,
    title,
    focus,
    summary_text,
    tokenize = 'unicode61'
);

CREATE TRIGGER IF NOT EXISTS snapshots_ai AFTER INSERT ON snapshots BEGIN
    INSERT INTO snapshots_fts(rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.focus, ''),
        COALESCE(new.summary_text, '')
    );
END;

CREATE TRIGGER IF NOT EXISTS snapshots_ad AFTER DELETE ON snapshots BEGIN
    INSERT INTO snapshots_fts(snapshots_fts, rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        'delete',
        old.rowid,
        old.id,
        COALESCE(old.title, ''),
        COALESCE(old.focus, ''),
        COALESCE(old.summary_text, '')
    );
END;

CREATE TRIGGER IF NOT EXISTS snapshots_au AFTER UPDATE ON snapshots BEGIN
    INSERT INTO snapshots_fts(snapshots_fts, rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        'delete',
        old.rowid,
        old.id,
        COALESCE(old.title, ''),
        COALESCE(old.focus, ''),
        COALESCE(old.summary_text, '')
    );
    INSERT INTO snapshots_fts(rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.focus, ''),
        COALESCE(new.summary_text, '')
    );
END;

CREATE TABLE IF NOT EXISTS ingest_runs (
    id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    source_ref TEXT,
    source_hash TEXT,
    items_seen INTEGER NOT NULL DEFAULT 0,
    items_written INTEGER NOT NULL DEFAULT 0,
    error_text TEXT,
    metadata_json TEXT,
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_ingest_runs_adapter ON ingest_runs(adapter, started_at_utc);

CREATE TABLE IF NOT EXISTS mem0_outbox (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    collection_name TEXT,
    operation TEXT NOT NULL DEFAULT 'add',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at_utc TEXT,
    last_error TEXT,
    payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    synced_at_utc TEXT,
    UNIQUE(event_id, operation),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
    CHECK(json_valid(payload_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_mem0_outbox_status ON mem0_outbox(status, created_at_utc);

CREATE VIEW IF NOT EXISTS v_chronicle_timeline AS
SELECT
    id,
    'event' AS record_type,
    occurred_at_utc AS ts_utc,
    occurred_at_local AS ts_local,
    event_type AS kind,
    COALESCE(title, text) AS summary,
    entity_id AS subject_id
FROM events
UNION ALL
SELECT
    id,
    'snapshot' AS record_type,
    captured_at_utc AS ts_utc,
    captured_at_local AS ts_local,
    domain AS kind,
    COALESCE(title, label, summary_text) AS summary,
    NULL AS subject_id
FROM snapshots;

PRAGMA user_version = 1;
