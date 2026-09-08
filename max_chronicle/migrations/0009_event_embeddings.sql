-- Chronicle v9 — event_embeddings for native hybrid recall.
-- Stores float32 BLOBs inline in chronicle.db; cosine ranking done in pure
-- Python (scale is hundreds→low-thousands of events, <10 ms, zero deps).
--
-- Conventions (match 0001..0008):
--   * STRICT table, IF NOT EXISTS on every object (idempotent re-runs safe).
--   * ISO-8601 UTC timestamp with %fZ format.
--   * No PRAGMA user_version — db.apply_migrations() sets it from max(version).

CREATE TABLE IF NOT EXISTS event_embeddings (
    event_id       TEXT    NOT NULL PRIMARY KEY
                           REFERENCES events(id) ON DELETE CASCADE,
    model          TEXT    NOT NULL,
    dim            INTEGER NOT NULL,
    vector         BLOB    NOT NULL,
    created_at_utc TEXT    NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_event_embeddings_model
    ON event_embeddings(model, created_at_utc DESC);
