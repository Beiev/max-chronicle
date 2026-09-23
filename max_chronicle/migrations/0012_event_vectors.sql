-- 0012: key event vectors by embedding model, so two indexes can coexist.
--
-- event_embeddings held one vector per event, so changing the embedding model
-- meant discarding the old index before the new one existed. event_vectors
-- keys each vector by (event, model_key): recall reads only the active
-- profile's key, and a new model is backfilled beside the old index. Existing
-- vectors keep their model name as their key; a vector whose event no longer
-- exists is dropped rather than failing the foreign key.

CREATE TABLE IF NOT EXISTS event_vectors (
    event_id       TEXT    NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    model_key      TEXT    NOT NULL,
    dim            INTEGER NOT NULL,
    vector         BLOB    NOT NULL,
    created_at_utc TEXT    NOT NULL,
    PRIMARY KEY (event_id, model_key)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_event_vectors_model ON event_vectors(model_key);

INSERT OR IGNORE INTO event_vectors(event_id, model_key, dim, vector, created_at_utc)
SELECT event_id, model, dim, vector, created_at_utc FROM event_embeddings
WHERE event_id IN (SELECT id FROM events);

DROP INDEX IF EXISTS idx_event_embeddings_model;
DROP TABLE IF EXISTS event_embeddings;
