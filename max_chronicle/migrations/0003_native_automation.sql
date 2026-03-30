CREATE TABLE IF NOT EXISTS automation_runs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    run_key TEXT,
    trigger_source TEXT NOT NULL DEFAULT 'manual',
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    details_json TEXT,
    event_id TEXT,
    snapshot_id TEXT,
    UNIQUE(job_name, run_key),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL,
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE SET NULL,
    CHECK(details_json IS NULL OR json_valid(details_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_automation_runs_job ON automation_runs(job_name, started_at_utc);

CREATE TABLE IF NOT EXISTS backup_runs (
    id TEXT PRIMARY KEY,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    target_root TEXT NOT NULL,
    backup_path TEXT,
    manifest_path TEXT,
    db_sha256 TEXT,
    artifact_files INTEGER NOT NULL DEFAULT 0,
    artifact_bytes INTEGER NOT NULL DEFAULT 0,
    restore_ok INTEGER,
    snapshot_id TEXT,
    restore_details_json TEXT,
    error_text TEXT,
    CHECK(restore_ok IS NULL OR restore_ok IN (0, 1)),
    CHECK(restore_details_json IS NULL OR json_valid(restore_details_json)),
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE SET NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_backup_runs_started ON backup_runs(started_at_utc);
CREATE INDEX IF NOT EXISTS idx_backup_runs_status ON backup_runs(status, started_at_utc);

CREATE TABLE IF NOT EXISTS hook_events (
    id TEXT PRIMARY KEY,
    hook_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    triggered_at_utc TEXT NOT NULL,
    source_ref TEXT,
    status TEXT NOT NULL,
    payload_json TEXT,
    event_id TEXT,
    snapshot_id TEXT,
    UNIQUE(hook_type, dedupe_key),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL,
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE SET NULL,
    CHECK(payload_json IS NULL OR json_valid(payload_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_hook_events_type ON hook_events(hook_type, triggered_at_utc);

CREATE TABLE IF NOT EXISTS curation_runs (
    id TEXT PRIMARY KEY,
    curation_type TEXT NOT NULL,
    run_key TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    model_name TEXT,
    prompt_sha256 TEXT,
    payload_json TEXT,
    artifact_id TEXT,
    event_id TEXT,
    notes TEXT,
    UNIQUE(curation_type, run_key),
    FOREIGN KEY(artifact_id) REFERENCES artifacts(id) ON DELETE SET NULL,
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL,
    CHECK(payload_json IS NULL OR json_valid(payload_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_curation_runs_type ON curation_runs(curation_type, started_at_utc);

CREATE TABLE IF NOT EXISTS event_external_refs (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    ref_type TEXT NOT NULL,
    ref_value TEXT NOT NULL,
    metadata_json TEXT,
    created_at_utc TEXT NOT NULL,
    UNIQUE(ref_type, ref_value),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_event_external_refs_event ON event_external_refs(event_id, ref_type);

PRAGMA user_version = 3;
