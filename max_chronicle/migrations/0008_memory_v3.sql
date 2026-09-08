-- Chronicle v8 foundation — Memory v3.
-- Ships Phase 1 surface (entity_aliases, session_messages, events.content_hash)
-- together with Phase 2 placeholder schema (facts, episodes, fact_*,
-- observability tables, generalized recall_outbox). Phase 2 tables stay empty
-- until record_episode lands; having the schema in 0008 means Phase 2 is a
-- pure "add MCP tools" sweep without another structural migration.
--
-- Conventions (match 0001..0007):
--   * STRICT tables, json_valid CHECKs, ISO-8601 UTC timestamps with %fZ.
--   * IF NOT EXISTS on every table + index (idempotent re-runs safe).
--   * No PRAGMA user_version — db.apply_migrations() sets it from max(version).

-- =========================================================================
-- Phase 1 — populated from day one
-- =========================================================================

-- 1.1  entity_aliases — first-class alias layer with normalized key + domain.
-- Fixes Ivan/Ivan/Иван fragmentation. alias_key is NFKC+casefold+collapsed
-- in application code; the DB only enforces uniqueness of active aliases.
CREATE TABLE IF NOT EXISTS entity_aliases (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_entity_id TEXT    NOT NULL,
    entity_type         TEXT    NOT NULL,
    domain              TEXT    NOT NULL DEFAULT 'global',
    alias_text          TEXT    NOT NULL,
    alias_key           TEXT    NOT NULL,
    alias_key_kind      TEXT,
    confidence          REAL    NOT NULL DEFAULT 1.0,
    status              TEXT    NOT NULL DEFAULT 'active'
                            CHECK(status IN ('active','inactive','rejected')),
    source              TEXT,
    source_refs_json    TEXT    NOT NULL DEFAULT '[]',
    created_at_utc      TEXT    NOT NULL,
    updated_at_utc      TEXT    NOT NULL,
    FOREIGN KEY(canonical_entity_id) REFERENCES normalized_entities(id) ON DELETE CASCADE,
    CHECK(json_valid(source_refs_json))
) STRICT;

-- Partial-unique guard: only active aliases must be unique per (domain, type, key).
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_aliases_active
    ON entity_aliases(domain, entity_type, alias_key)
    WHERE status='active';

CREATE INDEX IF NOT EXISTS idx_entity_aliases_canonical
    ON entity_aliases(canonical_entity_id, status);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_lookup
    ON entity_aliases(alias_key, entity_type)
    WHERE status='active';

-- 1.2  session_messages — agent extraction context ring (populated by Phase 2
-- record_episode; created now to keep 0008 idempotent at the schema level).
CREATE TABLE IF NOT EXISTS session_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_scope   TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    role            TEXT    NOT NULL CHECK(role IN ('user','assistant','tool')),
    content         TEXT    NOT NULL,
    created_at_utc  TEXT    NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_session_messages_scope_seq
    ON session_messages(session_scope, seq DESC);

-- 1.3  events.content_hash — 24h dedup for record_event. The column itself is
-- added by a Python pre-hook in db.apply_migrations (see _prepare_migration_v8)
-- because SQLite ALTER TABLE ADD COLUMN cannot be expressed idempotently in
-- pure SQL, and some live DBs already have the column from an earlier hotfix.
-- The index creation below is idempotent once the column exists.
CREATE INDEX IF NOT EXISTS idx_events_content_hash
    ON events(content_hash, occurred_at_utc DESC);

-- =========================================================================
-- Phase 2 — placeholder schema (empty until record_episode lands)
-- =========================================================================

-- 2.1  fact_transactions — logical mutation grouping for audit.
CREATE TABLE IF NOT EXISTS fact_transactions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    domain            TEXT    NOT NULL,
    operation         TEXT    NOT NULL
                          CHECK(operation IN ('record_episode','reconcile',
                                              'correct','merge','split',
                                              'retire','redact','restore')),
    request_id        TEXT    UNIQUE,
    actor             TEXT,
    episode_id        TEXT,
    recorded_at_utc   TEXT    NOT NULL,
    reason            TEXT,
    metadata_json     TEXT,
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fact_transactions_domain_time
    ON fact_transactions(domain, recorded_at_utc DESC);

-- 2.2  episodes — raw content provenance with SHA-256 dedup.
CREATE TABLE IF NOT EXISTS episodes (
    id                  TEXT    PRIMARY KEY,
    domain              TEXT    NOT NULL,
    group_id            TEXT,
    source              TEXT    NOT NULL,
    source_description  TEXT,
    content             TEXT,
    content_hash        TEXT    NOT NULL,
    content_redacted    INTEGER NOT NULL DEFAULT 0 CHECK(content_redacted IN (0,1)),
    created_at_utc      TEXT    NOT NULL,
    valid_at_utc        TEXT    NOT NULL,
    metadata_json       TEXT,
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_episodes_domain_valid
    ON episodes(domain, valid_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_episodes_hash
    ON episodes(content_hash);

-- 2.3  facts — bitemporal assertion store. Four time axes + slot/cardinality
-- for conflict policy. row_id is the FTS external-content rowid anchor; id is
-- the stable UUID used by FKs.
CREATE TABLE IF NOT EXISTS facts (
    row_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    id                   TEXT    NOT NULL UNIQUE,
    domain               TEXT    NOT NULL,
    group_id             TEXT,
    source_entity_id     TEXT,
    target_entity_id     TEXT,
    relation             TEXT    NOT NULL,
    fact_text            TEXT    NOT NULL,
    fact_hash            TEXT    NOT NULL,
    recorded_at_utc      TEXT    NOT NULL,
    expired_at_utc       TEXT,
    valid_from_utc       TEXT,
    valid_to_utc         TEXT,
    valid_from_precision TEXT    CHECK(valid_from_precision IS NULL
                                       OR valid_from_precision IN
                                          ('instant','day','month','year','unknown')),
    valid_from_key       TEXT    GENERATED ALWAYS AS
                                 (COALESCE(valid_from_utc, '0000-01-01T00:00:00.000Z')) STORED,
    valid_to_key         TEXT    GENERATED ALWAYS AS
                                 (COALESCE(valid_to_utc,   '9999-12-31T23:59:59.999Z')) STORED,
    created_tx_id        INTEGER NOT NULL,
    expired_tx_id        INTEGER,
    slot_key             TEXT    NOT NULL,
    value_key            TEXT,
    cardinality          TEXT    CHECK(cardinality IS NULL
                                       OR cardinality IN ('single','multi')),
    status               TEXT    NOT NULL DEFAULT 'active'
                                 CHECK(status IN ('active','retired','merged',
                                                  'redacted','purged')),
    confidence           REAL    DEFAULT 1.0,
    attributes_json      TEXT,
    attributed_to        TEXT,
    reference_time_utc   TEXT,
    FOREIGN KEY(source_entity_id) REFERENCES normalized_entities(id) ON DELETE SET NULL,
    FOREIGN KEY(target_entity_id) REFERENCES normalized_entities(id) ON DELETE SET NULL,
    FOREIGN KEY(created_tx_id)    REFERENCES fact_transactions(id),
    FOREIGN KEY(expired_tx_id)    REFERENCES fact_transactions(id),
    CHECK(attributes_json IS NULL OR json_valid(attributes_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_facts_valid      ON facts(domain, valid_from_key, valid_to_key);
CREATE INDEX IF NOT EXISTS idx_facts_system     ON facts(domain, recorded_at_utc, expired_at_utc);
CREATE INDEX IF NOT EXISTS idx_facts_active     ON facts(domain, status, expired_at_utc);
CREATE INDEX IF NOT EXISTS idx_facts_entities   ON facts(source_entity_id, target_entity_id, relation);
CREATE INDEX IF NOT EXISTS idx_facts_hash       ON facts(domain, fact_hash);
CREATE INDEX IF NOT EXISTS idx_facts_slot       ON facts(domain, slot_key, value_key);

-- 2.4  fact_observations — M:N facts↔episodes with per-observation metadata.
CREATE TABLE IF NOT EXISTS fact_observations (
    fact_id              TEXT    NOT NULL,
    episode_id           TEXT    NOT NULL,
    reference_time_utc   TEXT    NOT NULL,
    episode_valid_at_utc TEXT    NOT NULL,
    extractor_version    TEXT,
    confidence           REAL,
    PRIMARY KEY (fact_id, episode_id),
    FOREIGN KEY(fact_id)    REFERENCES facts(id)    ON DELETE CASCADE,
    FOREIGN KEY(episode_id) REFERENCES episodes(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fact_observations_episode
    ON fact_observations(episode_id);

-- 2.5  fact_supersessions — M:N old→new mutation tracking.
CREATE TABLE IF NOT EXISTS fact_supersessions (
    old_fact_id  TEXT    NOT NULL,
    new_fact_id  TEXT    NOT NULL,
    reason       TEXT    NOT NULL
                         CHECK(reason IN ('contradiction','correction','merge',
                                          'split','redaction','manual')),
    tx_id        INTEGER NOT NULL,
    PRIMARY KEY (old_fact_id, new_fact_id, reason),
    FOREIGN KEY(old_fact_id) REFERENCES facts(id)             ON DELETE CASCADE,
    FOREIGN KEY(new_fact_id) REFERENCES facts(id)             ON DELETE CASCADE,
    FOREIGN KEY(tx_id)       REFERENCES fact_transactions(id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fact_supersessions_new
    ON fact_supersessions(new_fact_id);

CREATE INDEX IF NOT EXISTS idx_fact_supersessions_tx
    ON fact_supersessions(tx_id);

-- 2.6  facts_fts — BM25 retrieval over fact_text + relation. External content,
-- synced via triggers. Safe on empty facts table.
CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
    fact_text,
    relation,
    content='facts',
    content_rowid='row_id',
    tokenize='unicode61'
);

CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
    INSERT INTO facts_fts(rowid, fact_text, relation)
    VALUES (NEW.row_id, NEW.fact_text, NEW.relation);
END;

CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
    INSERT INTO facts_fts(facts_fts, rowid, fact_text, relation)
    VALUES ('delete', OLD.row_id, OLD.fact_text, OLD.relation);
END;

CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN
    INSERT INTO facts_fts(facts_fts, rowid, fact_text, relation)
    VALUES ('delete', OLD.row_id, OLD.fact_text, OLD.relation);
    INSERT INTO facts_fts(rowid, fact_text, relation)
    VALUES (NEW.row_id, NEW.fact_text, NEW.relation);
END;

-- 2.7  recall_outbox — generalized sync queue (events, facts, entities). Not
-- replacing mem0_outbox yet — they coexist until Phase 2 flips writers.
CREATE TABLE IF NOT EXISTS recall_outbox (
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

CREATE INDEX IF NOT EXISTS idx_recall_outbox_status
    ON recall_outbox(status, domain, created_at_utc);

CREATE INDEX IF NOT EXISTS idx_recall_outbox_entity
    ON recall_outbox(entity_kind, entity_id);

-- 2.8  llm_calls — per-call cost + latency observability.
CREATE TABLE IF NOT EXISTS llm_calls (
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

CREATE INDEX IF NOT EXISTS idx_llm_calls_domain_op
    ON llm_calls(domain, operation, recorded_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_llm_calls_cost_day
    ON llm_calls(recorded_at_utc, cost_usd);

-- 2.9  fact_mutation_log — append-only audit for every fact CRUD.
CREATE TABLE IF NOT EXISTS fact_mutation_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    domain            TEXT    NOT NULL,
    action            TEXT    NOT NULL
                          CHECK(action IN ('create','update','retire',
                                           'redact','merge','split','restore')),
    fact_id           TEXT,
    previous_fact_id  TEXT,
    tx_id             INTEGER,
    reason            TEXT,
    recorded_at_utc   TEXT    NOT NULL,
    FOREIGN KEY(tx_id) REFERENCES fact_transactions(id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fact_mutation_log_fact
    ON fact_mutation_log(fact_id, recorded_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fact_mutation_log_tx
    ON fact_mutation_log(tx_id);

-- 2.10  reconciliation_runs — per-episode reconciler stats.
CREATE TABLE IF NOT EXISTS reconciliation_runs (
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

CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_episode
    ON reconciliation_runs(episode_id);

-- =========================================================================
-- Views
-- =========================================================================

-- current_facts — "what is true right now and still trusted". Uses
-- strftime with %fZ to match Chronicle's ISO format and excludes facts with
-- future valid_from to avoid premature activation.
CREATE VIEW IF NOT EXISTS current_facts AS
SELECT f.*
FROM facts AS f
WHERE f.status = 'active'
  AND f.expired_at_utc IS NULL
  AND (f.valid_from_utc IS NULL
       OR f.valid_from_utc <= strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  AND (f.valid_to_utc   IS NULL
       OR f.valid_to_utc   >  strftime('%Y-%m-%dT%H:%M:%fZ','now'));
