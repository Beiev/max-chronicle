-- Independent agent observations, request receipts, and task checkpoints.
-- Event history and existing IDs remain intact during upgrade.
CREATE TABLE event_observations (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    event_id TEXT NOT NULL REFERENCES events(id),
    request_id TEXT UNIQUE,
    request_hash TEXT NOT NULL,
    observation_hash TEXT NOT NULL,
    domain TEXT NOT NULL,
    project TEXT,
    task_id TEXT,
    session_id TEXT,
    actor TEXT,
    recorded_at_utc TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    UNIQUE(event_id, observation_hash)
) STRICT;

CREATE INDEX idx_observations_scope ON event_observations(domain, project, task_id, seq);
CREATE INDEX idx_observations_event ON event_observations(event_id, seq);
CREATE INDEX idx_events_task ON events(title, json_extract(payload_json, '$.task_id'), occurred_at_utc);
CREATE INDEX idx_facts_event ON facts(json_extract(attributes_json, '$.event_id'), status);

INSERT INTO event_observations(
    id, event_id, request_hash, observation_hash, domain, project, task_id,
    session_id, actor, recorded_at_utc, payload_json
)
SELECT id, id, '', id, COALESCE(circumstances, 'global'), title,
       json_extract(payload_json, '$.task_id'), json_extract(payload_json, '$.session_id'),
       actor, recorded_at_utc, payload_json
FROM events ORDER BY recorded_at_utc, id;
