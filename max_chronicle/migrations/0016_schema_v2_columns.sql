-- 0016: columns that say what they hold.
--
-- events.title held the project and events.circumstances the domain; task_id
-- and the quarantine flag lived only in payload_json. facts kept project,
-- task, kind and their event in attributes_json and group_id, and carried
-- columns nobody fills. db._prepare_migration_v16 refuses a database where
-- the dropped columns hold anything the remaining ones cannot give back.
--
-- Only ALTER TABLE: events and facts are parents of ON DELETE CASCADE
-- children, so rebuilding them inside this transaction would delete those
-- children. The code keeps writing payload_json.memory_guard and
-- attributes_json as well, so migrations/down/0016 can return to v15.

DROP VIEW IF EXISTS v_chronicle_timeline;                 -- read by nothing

-- events -------------------------------------------------------------------
DROP INDEX IF EXISTS idx_events_occurred_at_utc;          -- same column as idx_events_occurred_utc
DROP INDEX IF EXISTS idx_events_payload_domain_recent;    -- for rows without the column; none left
DROP INDEX IF EXISTS idx_events_payload_project_recent;
DROP INDEX IF EXISTS idx_events_title_recent;
DROP INDEX IF EXISTS idx_events_domain_recent;
DROP INDEX IF EXISTS idx_events_task;

ALTER TABLE events RENAME COLUMN title TO project;
ALTER TABLE events RENAME COLUMN circumstances TO domain;
ALTER TABLE events RENAME COLUMN imported_from TO written_by;
ALTER TABLE events ADD COLUMN task_id TEXT;
ALTER TABLE events ADD COLUMN visibility TEXT NOT NULL DEFAULT 'default'
    CHECK(visibility IN ('default', 'raw_only'));

-- The FTS triggers fire on UPDATE; text and why do not change, so the index
-- is rewritten with the same content.
UPDATE events SET
    project = COALESCE(project, json_extract(payload_json, '$.project')),
    domain = COALESCE(domain, json_extract(payload_json, '$.domain')),
    task_id = json_extract(payload_json, '$.task_id'),
    visibility = COALESCE(json_extract(payload_json, '$.memory_guard.visibility'), 'default');

ALTER TABLE event_observations ADD COLUMN visibility TEXT NOT NULL DEFAULT 'default'
    CHECK(visibility IN ('default', 'raw_only'));
UPDATE event_observations SET
    visibility = COALESCE(json_extract(payload_json, '$.memory_guard.visibility'), 'default');

CREATE INDEX idx_events_project_recent ON events(project, occurred_at_utc DESC);
CREATE INDEX idx_events_domain_recent ON events(domain, occurred_at_utc DESC);
CREATE INDEX idx_events_task ON events(project, task_id, occurred_at_utc);

-- facts --------------------------------------------------------------------
ALTER TABLE facts ADD COLUMN project TEXT;
ALTER TABLE facts ADD COLUMN task_id TEXT;
ALTER TABLE facts ADD COLUMN kind TEXT
    CHECK(kind IS NULL OR kind IN ('observed', 'decision', 'assumption'));
ALTER TABLE facts ADD COLUMN event_id TEXT;
ALTER TABLE facts ADD COLUMN observation_id TEXT;

UPDATE facts SET
    project = json_extract(attributes_json, '$.project'),
    task_id = json_extract(attributes_json, '$.task_id'),
    kind = json_extract(attributes_json, '$.kind'),
    event_id = json_extract(attributes_json, '$.event_id'),
    observation_id = json_extract(attributes_json, '$.observation_id');

-- One active value per slot, on the columns instead of the group_id pair
-- (the pre-hook checked that the two agree).
DROP INDEX IF EXISTS idx_facts_one_active_slot;
CREATE UNIQUE INDEX idx_facts_one_active_slot ON facts(domain, ifnull(project, ''), ifnull(task_id, ''), slot_key)
WHERE status = 'active' AND expired_at_utc IS NULL AND cardinality = 'single';
DROP INDEX IF EXISTS idx_facts_event;
CREATE INDEX idx_facts_event ON facts(event_id, status);
DROP INDEX IF EXISTS idx_facts_entities;                  -- source/target entity are never set

-- relation repeats slot_key in every row. The full-text index is rebuilt on
-- slot_key (a virtual table: no foreign keys, recreated and refilled here).
DROP TRIGGER IF EXISTS facts_ai;
DROP TRIGGER IF EXISTS facts_au;
DROP TRIGGER IF EXISTS facts_ad;
DROP TABLE IF EXISTS facts_fts;
ALTER TABLE facts DROP COLUMN relation;
ALTER TABLE facts DROP COLUMN confidence;                 -- 1.0 in every row
ALTER TABLE facts DROP COLUMN reference_time_utc;         -- never set
ALTER TABLE facts DROP COLUMN group_id;                   -- the [project, task] pair, now columns
CREATE VIRTUAL TABLE facts_fts USING fts5(fact_text, slot_key, tokenize = 'unicode61');
INSERT INTO facts_fts(rowid, fact_text, slot_key)
SELECT row_id, replace(replace(fact_text, 'ё', 'е'), 'Ё', 'Е'), replace(replace(slot_key, 'ё', 'е'), 'Ё', 'Е')
FROM facts;
CREATE TRIGGER facts_ai AFTER INSERT ON facts BEGIN
    INSERT INTO facts_fts(rowid, fact_text, slot_key)
    VALUES (NEW.row_id, replace(replace(NEW.fact_text, 'ё', 'е'), 'Ё', 'Е'),
            replace(replace(NEW.slot_key, 'ё', 'е'), 'Ё', 'Е'));
END;
CREATE TRIGGER facts_ad AFTER DELETE ON facts BEGIN
    DELETE FROM facts_fts WHERE rowid = OLD.row_id;
END;
CREATE TRIGGER facts_au AFTER UPDATE ON facts BEGIN
    DELETE FROM facts_fts WHERE rowid = OLD.row_id;
    INSERT INTO facts_fts(rowid, fact_text, slot_key)
    VALUES (NEW.row_id, replace(replace(NEW.fact_text, 'ё', 'е'), 'Ё', 'Е'),
            replace(replace(NEW.slot_key, 'ё', 'е'), 'Ё', 'Е'));
END;
