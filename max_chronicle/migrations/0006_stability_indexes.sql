CREATE INDEX IF NOT EXISTS idx_events_domain_recent
ON events(circumstances, occurred_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_events_title_recent
ON events(title, occurred_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_events_payload_domain_recent
ON events(json_extract(payload_json, '$.domain'), occurred_at_utc DESC)
WHERE circumstances IS NULL;

CREATE INDEX IF NOT EXISTS idx_events_payload_project_recent
ON events(json_extract(payload_json, '$.project'), occurred_at_utc DESC)
WHERE entity_id IS NULL AND title IS NULL;

PRAGMA user_version = 6;
