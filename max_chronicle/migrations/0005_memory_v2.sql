CREATE TABLE IF NOT EXISTS normalized_entities (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    canonical_key TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    aliases_json TEXT NOT NULL,
    source_refs_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE(entity_type, canonical_key),
    CHECK(json_valid(aliases_json)),
    CHECK(json_valid(source_refs_json)),
    CHECK(metadata_json IS NULL OR json_valid(metadata_json))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_normalized_entities_type_key
ON normalized_entities(entity_type, canonical_key);

CREATE INDEX IF NOT EXISTS idx_normalized_entities_updated
ON normalized_entities(updated_at_utc DESC);

CREATE TABLE IF NOT EXISTS situation_models (
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

CREATE INDEX IF NOT EXISTS idx_situation_models_domain_valid
ON situation_models(domain, valid_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_situation_models_snapshot
ON situation_models(snapshot_id);

CREATE TABLE IF NOT EXISTS lens_runs (
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

CREATE INDEX IF NOT EXISTS idx_lens_runs_situation
ON lens_runs(situation_id, updated_at_utc DESC);

CREATE TABLE IF NOT EXISTS scenario_runs (
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

CREATE INDEX IF NOT EXISTS idx_scenario_runs_situation
ON scenario_runs(situation_id, updated_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_scenario_runs_review_due
ON scenario_runs(review_due_at_utc);

CREATE TABLE IF NOT EXISTS forecast_reviews (
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

CREATE INDEX IF NOT EXISTS idx_forecast_reviews_scenario
ON forecast_reviews(scenario_id, updated_at_utc DESC);

PRAGMA user_version = 5;
