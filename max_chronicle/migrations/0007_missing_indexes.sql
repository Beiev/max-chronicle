-- Standalone time-order index for events (covers ORDER BY occurred_at_utc
-- without relying on composite indexes from 0006).
CREATE INDEX IF NOT EXISTS idx_events_occurred_utc
ON events(occurred_at_utc DESC);

-- mem0_outbox: accelerates sync-mem0 pending scans and retention prune.
CREATE INDEX IF NOT EXISTS idx_outbox_status
ON mem0_outbox(status, created_at_utc);

-- situation_models: accelerates fetch_latest_situation_model per domain.
CREATE INDEX IF NOT EXISTS idx_situation_domain_valid
ON situation_models(domain, valid_at_utc DESC);

-- snapshots: accelerates fetch_latest_snapshot per domain.
CREATE INDEX IF NOT EXISTS idx_snapshots_domain_captured
ON snapshots(domain, captured_at_utc DESC);

PRAGMA user_version = 7;
