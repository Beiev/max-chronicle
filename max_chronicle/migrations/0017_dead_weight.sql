-- 0017: tables, indexes and copies that nothing reads.
--
-- The copy the upgrade takes first (chronicle.db.bak-premigrate-v16-v17-*)
-- keeps all of it; migrations/down/0017 brings it back from that copy.
-- Vectors of an embedding model the installation no longer uses stay here:
-- `chronicle embed-prune` removes them when asked.

-- Never written, never read.
DROP TABLE IF EXISTS llm_calls;
DROP TABLE IF EXISTS recall_outbox;
DROP TABLE IF EXISTS reconciliation_runs;
DROP TABLE IF EXISTS session_messages;

-- Situation models, lens runs, scenarios and forecast reviews: no code path
-- writes or reads them any more. Children first.
DROP TABLE IF EXISTS forecast_reviews;
DROP TABLE IF EXISTS scenario_runs;
DROP TABLE IF EXISTS lens_runs;
DROP TABLE IF EXISTS situation_models;

-- The snapshot full-text index: kept current by triggers, queried by nothing.
DROP TRIGGER IF EXISTS snapshots_ai;
DROP TRIGGER IF EXISTS snapshots_au;
DROP TRIGGER IF EXISTS snapshots_ad;
DROP TABLE IF EXISTS snapshots_fts;

-- Duplicate indexes.
DROP INDEX IF EXISTS idx_outbox_status;      -- = idx_mem0_outbox_status
DROP INDEX IF EXISTS idx_snapshots_domain;   -- idx_snapshots_domain_captured covers it

-- A snapshot keeps the ids of the events it saw instead of copies of them, so
-- a reader takes each event as it is now and one quarantined since stays out.
-- Only an entry with the id of an event in this database becomes that id;
-- anything else stays as it was. json_each yields the entries in order, and
-- json() around every element keeps each one JSON, not a string.
UPDATE snapshots SET payload_json = json_set(payload_json, '$.recent_ledger', json((
    SELECT json_group_array(json(CASE
        WHEN entry.type = 'object' AND json_type(entry.value, '$.id') = 'text'
             AND EXISTS (SELECT 1 FROM events WHERE events.id = json_extract(entry.value, '$.id'))
            THEN json_quote(json_extract(entry.value, '$.id'))
        ELSE snapshots.payload_json -> entry.fullkey
    END))
    FROM json_each(snapshots.payload_json, '$.recent_ledger') AS entry)))
WHERE json_type(payload_json, '$.recent_ledger') = 'array';

-- Copies of Mem0 recall taken at capture time; a capture with Mem0 on writes
-- fresh ones. mem0_dump stays: its enabled=false tells the activation prompt
-- that Mem0 is off.
UPDATE snapshots SET payload_json = json_remove(payload_json, '$.mem0_snapshot_hits')
WHERE json_type(payload_json, '$.mem0_snapshot_hits') IS NOT NULL;
