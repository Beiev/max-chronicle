-- 0018: Chronicle no longer mirrors events into Mem0 (0.26).
--
-- The outbox, the two event columns and the sync keys in event, observation
-- and snapshot payloads go. The copy the upgrade takes first
-- (chronicle.db.bak-premigrate-v17-v18-*) keeps all of it;
-- migrations/down/0018 brings it back from that copy. Artifacts, runs and
-- entities that name Mem0 are history and stay.

DROP TABLE IF EXISTS mem0_outbox;   -- and idx_mem0_outbox_status with it

ALTER TABLE events DROP COLUMN mem0_status;
ALTER TABLE events DROP COLUMN mem0_error;

UPDATE events
SET payload_json = json_remove(payload_json, '$.mem0_status', '$.mem0_error', '$.mem0_raw', '$.mem0_synced_at')
WHERE json_type(payload_json, '$.mem0_status') IS NOT NULL
   OR json_type(payload_json, '$.mem0_error') IS NOT NULL
   OR json_type(payload_json, '$.mem0_raw') IS NOT NULL
   OR json_type(payload_json, '$.mem0_synced_at') IS NOT NULL;

-- An observation keeps the entry as the agent sent it; the sync keys were
-- Chronicle's, not the agent's, and no request hash covers them.
UPDATE event_observations
SET payload_json = json_remove(payload_json, '$.mem0_status', '$.mem0_error', '$.mem0_raw', '$.mem0_synced_at')
WHERE json_type(payload_json, '$.mem0_status') IS NOT NULL
   OR json_type(payload_json, '$.mem0_error') IS NOT NULL
   OR json_type(payload_json, '$.mem0_raw') IS NOT NULL
   OR json_type(payload_json, '$.mem0_synced_at') IS NOT NULL;

-- What a capture knew of the Mem0 dump.
UPDATE snapshots SET payload_json = json_remove(payload_json, '$.mem0_dump')
WHERE json_type(payload_json, '$.mem0_dump') IS NOT NULL;
