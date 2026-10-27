-- Revert 0018: back to schema v17, as v0.25 reads it
-- (`chronicle migrate --revert 18 --archive PATH`).
--
-- Reads the copy the upgrade took first (chronicle.db.bak-premigrate-v17-v18-*),
-- which the runner attaches as `archive`: the outbox, the event columns and
-- the payload keys come back from it. A payload 0018 rewrote and nothing has
-- touched since gets the copy's text back; one changed since gets the keys
-- alone. After an upgrade from v16 straight to v18 one copy, taken before
-- 0017, serves both reverts: a snapshot then gets the copy's v16 payload here,
-- which v0.25 reads too, and the revert of 0017 finds nothing left to do.
-- The two columns come back at the end of `events`; v0.25 names its columns.
-- The runner removes version 18 from schema_migrations and sets user_version.

-- Outbox ----------------------------------------------------------------------
CREATE TABLE mem0_outbox (
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

CREATE INDEX idx_mem0_outbox_status ON mem0_outbox(status, created_at_utc);

INSERT INTO mem0_outbox(id, event_id, collection_name, operation, status, attempts,
                        last_attempt_at_utc, last_error, payload_json, created_at_utc, synced_at_utc)
SELECT id, event_id, collection_name, operation, status, attempts,
       last_attempt_at_utc, last_error, payload_json, created_at_utc, synced_at_utc
FROM archive.mem0_outbox;

-- Events ----------------------------------------------------------------------
ALTER TABLE events ADD COLUMN mem0_status TEXT;
ALTER TABLE events ADD COLUMN mem0_error TEXT;

UPDATE events SET mem0_status = copy.mem0_status, mem0_error = copy.mem0_error
FROM archive.events AS copy
WHERE copy.id = events.id;

UPDATE events SET payload_json = copy.payload_json
FROM archive.events AS copy
WHERE copy.id = events.id
  AND events.payload_json = json_remove(copy.payload_json,
      '$.mem0_status', '$.mem0_error', '$.mem0_raw', '$.mem0_synced_at');

UPDATE events SET payload_json = json_set(events.payload_json, '$.mem0_status', json(copy.payload_json -> '$.mem0_status'))
FROM archive.events AS copy
WHERE copy.id = events.id AND json_type(copy.payload_json, '$.mem0_status') IS NOT NULL
  AND json_type(events.payload_json, '$.mem0_status') IS NULL;
UPDATE events SET payload_json = json_set(events.payload_json, '$.mem0_error', json(copy.payload_json -> '$.mem0_error'))
FROM archive.events AS copy
WHERE copy.id = events.id AND json_type(copy.payload_json, '$.mem0_error') IS NOT NULL
  AND json_type(events.payload_json, '$.mem0_error') IS NULL;
UPDATE events SET payload_json = json_set(events.payload_json, '$.mem0_raw', json(copy.payload_json -> '$.mem0_raw'))
FROM archive.events AS copy
WHERE copy.id = events.id AND json_type(copy.payload_json, '$.mem0_raw') IS NOT NULL
  AND json_type(events.payload_json, '$.mem0_raw') IS NULL;
UPDATE events SET payload_json = json_set(events.payload_json, '$.mem0_synced_at', json(copy.payload_json -> '$.mem0_synced_at'))
FROM archive.events AS copy
WHERE copy.id = events.id AND json_type(copy.payload_json, '$.mem0_synced_at') IS NOT NULL
  AND json_type(events.payload_json, '$.mem0_synced_at') IS NULL;

-- Observations ----------------------------------------------------------------
-- No code path changes an observation, and v0.25 never reads these keys there.
UPDATE event_observations SET payload_json = copy.payload_json
FROM archive.event_observations AS copy
WHERE copy.id = event_observations.id
  AND event_observations.payload_json = json_remove(copy.payload_json,
      '$.mem0_status', '$.mem0_error', '$.mem0_raw', '$.mem0_synced_at');

-- Snapshots -------------------------------------------------------------------
-- The copy's payload where this one is the copy less mem0_dump (and, from a
-- copy taken before 0017, less what 0017 rewrote: the ledger, as long, and
-- mem0_snapshot_hits).
UPDATE snapshots SET payload_json = copy.payload_json
FROM archive.snapshots AS copy
WHERE copy.id = snapshots.id
  AND json_remove(snapshots.payload_json, '$.recent_ledger')
      = json_remove(copy.payload_json, '$.recent_ledger', '$.mem0_snapshot_hits', '$.mem0_dump')
  AND json_array_length(snapshots.payload_json, '$.recent_ledger')
      IS json_array_length(copy.payload_json, '$.recent_ledger');

UPDATE snapshots SET payload_json = json_set(snapshots.payload_json, '$.mem0_dump', json(copy.payload_json -> '$.mem0_dump'))
FROM archive.snapshots AS copy
WHERE copy.id = snapshots.id AND json_type(copy.payload_json, '$.mem0_dump') IS NOT NULL
  AND json_type(snapshots.payload_json, '$.mem0_dump') IS NULL;

-- A snapshot captured since ran without Mem0, and says so: v0.25 takes a
-- snapshot without mem0_dump for one with Mem0 on.
UPDATE snapshots SET payload_json = json_set(payload_json, '$.mem0_dump', json('{"enabled":false}'))
WHERE json_type(payload_json, '$.mem0_dump') IS NULL
  AND id NOT IN (SELECT id FROM archive.snapshots);
