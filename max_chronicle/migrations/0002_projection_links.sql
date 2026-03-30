CREATE TABLE IF NOT EXISTS projection_runs (
    id TEXT PRIMARY KEY,
    projection_name TEXT NOT NULL,
    target_path TEXT NOT NULL,
    snapshot_id TEXT,
    rendered_at_utc TEXT NOT NULL,
    status TEXT NOT NULL,
    content_sha256 TEXT,
    metadata_json TEXT,
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id),
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_projection_runs_name
ON projection_runs(projection_name, rendered_at_utc DESC);

CREATE TABLE IF NOT EXISTS artifact_links (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    link_role TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    metadata_json TEXT,
    UNIQUE(artifact_id, target_type, target_id, link_role),
    FOREIGN KEY(artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE,
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_artifact_links_target
ON artifact_links(target_type, target_id, link_role);

CREATE INDEX IF NOT EXISTS idx_artifacts_entity_observed
ON artifacts(entity_id, observed_at_utc);

CREATE INDEX IF NOT EXISTS idx_relations_from_type
ON relations(from_entity_id, relation_type, created_at_utc);

PRAGMA user_version = 2;
