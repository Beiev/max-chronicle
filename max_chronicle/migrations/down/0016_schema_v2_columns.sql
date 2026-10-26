-- Revert 0016: back to schema v15, as v0.23 reads it (`chronicle migrate --revert 16`).
--
-- Every dropped column comes back from the ones that stay: relation is
-- slot_key, confidence 1.0, reference_time_utc empty, group_id the
-- [project, task_id] pair. payload_json.memory_guard and attributes_json were
-- written alongside the columns, so nothing written since 0016 is lost.
-- The runner removes version 16 from schema_migrations and sets user_version.

-- facts --------------------------------------------------------------------
DROP TRIGGER IF EXISTS facts_ai;
DROP TRIGGER IF EXISTS facts_au;
DROP TRIGGER IF EXISTS facts_ad;
DROP TABLE IF EXISTS facts_fts;
DROP INDEX IF EXISTS idx_facts_one_active_slot;
DROP INDEX IF EXISTS idx_facts_event;

ALTER TABLE facts ADD COLUMN relation TEXT NOT NULL DEFAULT '';
ALTER TABLE facts ADD COLUMN confidence REAL DEFAULT 1.0;
ALTER TABLE facts ADD COLUMN reference_time_utc TEXT;
ALTER TABLE facts ADD COLUMN group_id TEXT;
UPDATE facts SET relation = slot_key, confidence = 1.0, group_id = json_array(project, task_id);

ALTER TABLE facts DROP COLUMN project;
ALTER TABLE facts DROP COLUMN task_id;
ALTER TABLE facts DROP COLUMN kind;
ALTER TABLE facts DROP COLUMN event_id;
ALTER TABLE facts DROP COLUMN observation_id;

CREATE INDEX idx_facts_entities   ON facts(source_entity_id, target_entity_id, relation);
CREATE INDEX idx_facts_event ON facts(json_extract(attributes_json, '$.event_id'), status);
CREATE UNIQUE INDEX idx_facts_one_active_slot ON facts(
    domain,
    ifnull(json_extract(group_id, '$[0]'), ''),
    ifnull(json_extract(group_id, '$[1]'), ''),
    slot_key)
WHERE status = 'active' AND expired_at_utc IS NULL AND cardinality = 'single'
  AND (group_id IS NULL OR json_valid(group_id));

CREATE VIRTUAL TABLE facts_fts USING fts5(
    fact_text,
    relation,
    tokenize = 'unicode61'
);

INSERT INTO facts_fts(rowid, fact_text, relation)
SELECT
    row_id,
    replace(replace(fact_text, 'ё', 'е'), 'Ё', 'Е'),
    replace(replace(COALESCE(relation, ''), 'ё', 'е'), 'Ё', 'Е')
FROM facts;

CREATE TRIGGER facts_ai AFTER INSERT ON facts BEGIN
    INSERT INTO facts_fts(rowid, fact_text, relation)
    VALUES (
        NEW.row_id,
        replace(replace(NEW.fact_text, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(COALESCE(NEW.relation, ''), 'ё', 'е'), 'Ё', 'Е')
    );
END;

CREATE TRIGGER facts_ad AFTER DELETE ON facts BEGIN
    DELETE FROM facts_fts WHERE rowid = OLD.row_id;
END;

CREATE TRIGGER facts_au AFTER UPDATE ON facts BEGIN
    DELETE FROM facts_fts WHERE rowid = OLD.row_id;
    INSERT INTO facts_fts(rowid, fact_text, relation)
    VALUES (
        NEW.row_id,
        replace(replace(NEW.fact_text, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(COALESCE(NEW.relation, ''), 'ё', 'е'), 'Ё', 'Е')
    );
END;

-- events -------------------------------------------------------------------
DROP INDEX IF EXISTS idx_events_project_recent;
DROP INDEX IF EXISTS idx_events_domain_recent;
DROP INDEX IF EXISTS idx_events_task;

ALTER TABLE event_observations DROP COLUMN visibility;
ALTER TABLE events DROP COLUMN task_id;
ALTER TABLE events DROP COLUMN visibility;
ALTER TABLE events RENAME COLUMN project TO title;
ALTER TABLE events RENAME COLUMN domain TO circumstances;
ALTER TABLE events RENAME COLUMN written_by TO imported_from;

CREATE INDEX idx_events_occurred_at_utc ON events(occurred_at_utc);
CREATE INDEX idx_events_domain_recent
ON events(circumstances, occurred_at_utc DESC);
CREATE INDEX idx_events_title_recent
ON events(title, occurred_at_utc DESC);
CREATE INDEX idx_events_payload_domain_recent
ON events(json_extract(payload_json, '$.domain'), occurred_at_utc DESC)
WHERE circumstances IS NULL;
CREATE INDEX idx_events_payload_project_recent
ON events(json_extract(payload_json, '$.project'), occurred_at_utc DESC)
WHERE entity_id IS NULL AND title IS NULL;
CREATE INDEX idx_events_task ON events(title, json_extract(payload_json, '$.task_id'), occurred_at_utc);

CREATE VIEW v_chronicle_timeline AS
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
