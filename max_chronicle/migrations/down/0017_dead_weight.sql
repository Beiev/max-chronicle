-- Revert 0017: back to schema v16, as v0.24 reads it
-- (`chronicle migrate --revert 17 --archive PATH`).
--
-- Reads the copy the upgrade took first (chronicle.db.bak-premigrate-v16-v17-*),
-- which the runner attaches as `archive`: the dropped tables and the snapshot
-- payloads come back from it. 0017 changed nothing else, so nothing written
-- since is lost. Snapshots captured since keep id-only ledgers, which v0.24
-- reads. The runner removes version 17 from schema_migrations and sets
-- user_version.

-- Situation layer, parents first ----------------------------------------------
CREATE TABLE situation_models (
    id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    snapshot_id TEXT,
    valid_at_utc TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    summary_text TEXT,
    payload_json TEXT NOT NULL,
    derived_from_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE SET NULL,
    CHECK(json_valid(payload_json)),
    CHECK(json_valid(derived_from_json))
) STRICT;
INSERT INTO situation_models(id, domain, snapshot_id, valid_at_utc, status, summary_text, payload_json,
                             derived_from_json, created_at_utc, updated_at_utc)
SELECT id, domain, snapshot_id, valid_at_utc, status, summary_text, payload_json,
       derived_from_json, created_at_utc, updated_at_utc
FROM archive.situation_models;
CREATE INDEX idx_situation_models_domain_valid
ON situation_models(domain, valid_at_utc DESC);
CREATE INDEX idx_situation_models_snapshot
ON situation_models(snapshot_id);
CREATE INDEX idx_situation_domain_valid
ON situation_models(domain, valid_at_utc DESC);

CREATE TABLE lens_runs (
    id TEXT PRIMARY KEY,
    situation_id TEXT NOT NULL,
    lens TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    confidence REAL NOT NULL,
    summary_text TEXT,
    findings_json TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE(situation_id, lens),
    FOREIGN KEY(situation_id) REFERENCES situation_models(id) ON DELETE CASCADE,
    CHECK(json_valid(findings_json)),
    CHECK(json_valid(evidence_refs_json))
) STRICT;
INSERT INTO lens_runs(id, situation_id, lens, status, confidence, summary_text, findings_json,
                      evidence_refs_json, created_at_utc, updated_at_utc)
SELECT id, situation_id, lens, status, confidence, summary_text, findings_json,
       evidence_refs_json, created_at_utc, updated_at_utc
FROM archive.lens_runs;
CREATE INDEX idx_lens_runs_situation
ON lens_runs(situation_id, updated_at_utc DESC);

CREATE TABLE scenario_runs (
    id TEXT PRIMARY KEY,
    situation_id TEXT NOT NULL,
    scenario_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    confidence REAL NOT NULL,
    review_due_at_utc TEXT,
    summary_text TEXT,
    assumptions_json TEXT NOT NULL,
    changed_variables_json TEXT NOT NULL,
    expected_outcomes_json TEXT NOT NULL,
    failure_modes_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(situation_id) REFERENCES situation_models(id) ON DELETE CASCADE,
    CHECK(json_valid(assumptions_json)),
    CHECK(json_valid(changed_variables_json)),
    CHECK(json_valid(expected_outcomes_json)),
    CHECK(json_valid(failure_modes_json)),
    CHECK(json_valid(payload_json)),
    CHECK(json_array_length(assumptions_json) > 0)
) STRICT;
INSERT INTO scenario_runs(id, situation_id, scenario_name, status, confidence, review_due_at_utc,
                          summary_text, assumptions_json, changed_variables_json, expected_outcomes_json,
                          failure_modes_json, payload_json, created_at_utc, updated_at_utc)
SELECT id, situation_id, scenario_name, status, confidence, review_due_at_utc,
       summary_text, assumptions_json, changed_variables_json, expected_outcomes_json,
       failure_modes_json, payload_json, created_at_utc, updated_at_utc
FROM archive.scenario_runs;
CREATE INDEX idx_scenario_runs_situation
ON scenario_runs(situation_id, updated_at_utc DESC);
CREATE INDEX idx_scenario_runs_review_due
ON scenario_runs(review_due_at_utc);

CREATE TABLE forecast_reviews (
    id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    outcome_event_id TEXT,
    status TEXT NOT NULL,
    review_summary TEXT,
    payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    FOREIGN KEY(scenario_id) REFERENCES scenario_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(outcome_event_id) REFERENCES events(id) ON DELETE SET NULL,
    CHECK(json_valid(payload_json))
) STRICT;
INSERT INTO forecast_reviews(id, scenario_id, outcome_event_id, status, review_summary, payload_json,
                             created_at_utc, updated_at_utc)
SELECT id, scenario_id, outcome_event_id, status, review_summary, payload_json,
       created_at_utc, updated_at_utc
FROM archive.forecast_reviews;
CREATE INDEX idx_forecast_reviews_scenario
ON forecast_reviews(scenario_id, updated_at_utc DESC);

-- Tables nothing wrote ----------------------------------------------------------
CREATE TABLE llm_calls (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    domain            TEXT    NOT NULL,
    episode_id        TEXT,
    operation         TEXT    NOT NULL,
    model             TEXT,
    tokens_input      INTEGER,
    tokens_output     INTEGER,
    cost_usd          REAL,
    latency_ms        INTEGER,
    error_class       TEXT,
    prompt_sha256     TEXT,
    recorded_at_utc   TEXT    NOT NULL,
    FOREIGN KEY(episode_id) REFERENCES episodes(id) ON DELETE SET NULL
) STRICT;
INSERT INTO llm_calls(id, domain, episode_id, operation, model, tokens_input, tokens_output, cost_usd,
                      latency_ms, error_class, prompt_sha256, recorded_at_utc)
SELECT id, domain, episode_id, operation, model, tokens_input, tokens_output, cost_usd,
       latency_ms, error_class, prompt_sha256, recorded_at_utc
FROM archive.llm_calls;
CREATE INDEX idx_llm_calls_domain_op
    ON llm_calls(domain, operation, recorded_at_utc DESC);
CREATE INDEX idx_llm_calls_cost_day
    ON llm_calls(recorded_at_utc, cost_usd);

CREATE TABLE recall_outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    domain          TEXT    NOT NULL,
    entity_kind     TEXT    NOT NULL
                        CHECK(entity_kind IN ('event','fact','entity','episode')),
    entity_id       TEXT    NOT NULL,
    collection_name TEXT,
    operation       TEXT    NOT NULL DEFAULT 'add'
                        CHECK(operation IN ('add','update','delete')),
    status          TEXT    NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','synced','skipped','failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at_utc TEXT,
    last_error      TEXT,
    payload_json    TEXT    NOT NULL,
    created_at_utc  TEXT    NOT NULL,
    updated_at_utc  TEXT    NOT NULL,
    CHECK(json_valid(payload_json))
) STRICT;
INSERT INTO recall_outbox(id, domain, entity_kind, entity_id, collection_name, operation, status, attempts,
                          last_attempt_at_utc, last_error, payload_json, created_at_utc, updated_at_utc)
SELECT id, domain, entity_kind, entity_id, collection_name, operation, status, attempts,
       last_attempt_at_utc, last_error, payload_json, created_at_utc, updated_at_utc
FROM archive.recall_outbox;
CREATE INDEX idx_recall_outbox_status
    ON recall_outbox(status, domain, created_at_utc);
CREATE INDEX idx_recall_outbox_entity
    ON recall_outbox(entity_kind, entity_id);

CREATE TABLE reconciliation_runs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    domain                TEXT    NOT NULL,
    episode_id            TEXT,
    candidate_count       INTEGER,
    invalidated_count     INTEGER,
    merged_count          INTEGER,
    split_count           INTEGER,
    contradictions_found  INTEGER,
    dry_run               INTEGER NOT NULL DEFAULT 0 CHECK(dry_run IN (0,1)),
    recorded_at_utc       TEXT    NOT NULL,
    FOREIGN KEY(episode_id) REFERENCES episodes(id) ON DELETE SET NULL
) STRICT;
INSERT INTO reconciliation_runs(id, domain, episode_id, candidate_count, invalidated_count, merged_count,
                                split_count, contradictions_found, dry_run, recorded_at_utc)
SELECT id, domain, episode_id, candidate_count, invalidated_count, merged_count,
       split_count, contradictions_found, dry_run, recorded_at_utc
FROM archive.reconciliation_runs;
CREATE INDEX idx_reconciliation_runs_episode
    ON reconciliation_runs(episode_id);

CREATE TABLE session_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_scope   TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    role            TEXT    NOT NULL CHECK(role IN ('user','assistant','tool')),
    content         TEXT    NOT NULL,
    created_at_utc  TEXT    NOT NULL
) STRICT;
INSERT INTO session_messages(id, session_scope, seq, role, content, created_at_utc)
SELECT id, session_scope, seq, role, content, created_at_utc
FROM archive.session_messages;
CREATE INDEX idx_session_messages_scope_seq
    ON session_messages(session_scope, seq DESC);

-- AUTOINCREMENT counters of the four tables above, as the copy had them.
DELETE FROM main.sqlite_sequence
WHERE name IN ('llm_calls', 'recall_outbox', 'reconciliation_runs', 'session_messages');
INSERT INTO main.sqlite_sequence(name, seq)
SELECT name, seq FROM archive.sqlite_sequence
WHERE name IN ('llm_calls', 'recall_outbox', 'reconciliation_runs', 'session_messages');

-- Snapshots ----------------------------------------------------------------------
-- Payloads as the copy had them, for the snapshots 0017 rewrote: the rest of
-- the payload is the copy's, less mem0_snapshot_hits, and the ledger as long.
-- A snapshot captured since, or stored again under its id, keeps its own.
UPDATE snapshots SET payload_json = (
    SELECT copy.payload_json FROM archive.snapshots AS copy WHERE copy.id = snapshots.id)
WHERE EXISTS (
    SELECT 1 FROM archive.snapshots AS copy
    WHERE copy.id = snapshots.id
      AND json_remove(snapshots.payload_json, '$.recent_ledger')
          = json_remove(copy.payload_json, '$.recent_ledger', '$.mem0_snapshot_hits')
      AND json_array_length(snapshots.payload_json, '$.recent_ledger')
          IS json_array_length(copy.payload_json, '$.recent_ledger'));

CREATE INDEX idx_snapshots_domain ON snapshots(domain, captured_at_utc);
CREATE INDEX idx_outbox_status
ON mem0_outbox(status, created_at_utc);

CREATE VIRTUAL TABLE snapshots_fts USING fts5(
    snapshot_id UNINDEXED,
    title,
    focus,
    summary_text,
    tokenize = 'unicode61'
);
INSERT INTO snapshots_fts(rowid, snapshot_id, title, focus, summary_text)
SELECT rowid, id, COALESCE(title, ''), COALESCE(focus, ''), COALESCE(summary_text, '')
FROM snapshots;

CREATE TRIGGER snapshots_ai AFTER INSERT ON snapshots BEGIN
    INSERT INTO snapshots_fts(rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.focus, ''),
        COALESCE(new.summary_text, '')
    );
END;

CREATE TRIGGER snapshots_ad AFTER DELETE ON snapshots BEGIN
    DELETE FROM snapshots_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER snapshots_au AFTER UPDATE ON snapshots BEGIN
    DELETE FROM snapshots_fts WHERE rowid = old.rowid;
    INSERT INTO snapshots_fts(rowid, snapshot_id, title, focus, summary_text)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        COALESCE(new.focus, ''),
        COALESCE(new.summary_text, '')
    );
END;
