-- 0013: an index of notes written on purpose (FR-10).
--
-- Agents keep durable notes as files (Claude's file memory). Chronicle indexes
-- them for recall and never writes to them: `documents` holds one row per note
-- file, `document_chunks` its sections. A deleted note keeps its row as a
-- tombstone (deleted_at_utc) and loses its chunks. `project` is the note's
-- canonical project, or NULL for a global note. Documents have no task or
-- domain. Chunk text is stored after the secret filter (FR-11). A note indexed
-- by an older parser or filter (index_version) is indexed again.
--
-- document_chunks_fts folds ё into е like events_fts (0011). chunk_vectors are
-- keyed by embedding model like event_vectors (0012).

CREATE TABLE IF NOT EXISTS documents (
    id              TEXT    PRIMARY KEY,
    path            TEXT    NOT NULL UNIQUE,
    source          TEXT    NOT NULL,
    project         TEXT,
    title           TEXT    NOT NULL,
    description     TEXT,
    kind            TEXT,
    content_sha256  TEXT    NOT NULL,
    size_bytes      INTEGER NOT NULL,
    modified_at_utc TEXT    NOT NULL,
    indexed_at_utc  TEXT    NOT NULL,
    deleted_at_utc  TEXT,
    redactions      INTEGER NOT NULL DEFAULT 0,
    index_version   INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE INDEX IF NOT EXISTS idx_documents_live ON documents(project) WHERE deleted_at_utc IS NULL;

CREATE TABLE IF NOT EXISTS document_chunks (
    id          TEXT    PRIMARY KEY,
    document_id TEXT    NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    ordinal     INTEGER NOT NULL,
    heading     TEXT    NOT NULL,
    text        TEXT    NOT NULL,
    UNIQUE (document_id, ordinal)
) STRICT;

CREATE VIRTUAL TABLE IF NOT EXISTS document_chunks_fts USING fts5(
    chunk_id UNINDEXED,
    heading,
    text,
    tokenize = 'unicode61'
);

CREATE TRIGGER IF NOT EXISTS document_chunks_ai AFTER INSERT ON document_chunks BEGIN
    INSERT INTO document_chunks_fts(rowid, chunk_id, heading, text)
    VALUES (
        new.rowid,
        new.id,
        replace(replace(new.heading, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(new.text, 'ё', 'е'), 'Ё', 'Е')
    );
END;

CREATE TRIGGER IF NOT EXISTS document_chunks_ad AFTER DELETE ON document_chunks BEGIN
    DELETE FROM document_chunks_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER IF NOT EXISTS document_chunks_au AFTER UPDATE ON document_chunks BEGIN
    DELETE FROM document_chunks_fts WHERE rowid = old.rowid;
    INSERT INTO document_chunks_fts(rowid, chunk_id, heading, text)
    VALUES (
        new.rowid,
        new.id,
        replace(replace(new.heading, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(new.text, 'ё', 'е'), 'Ё', 'Е')
    );
END;

CREATE TABLE IF NOT EXISTS chunk_vectors (
    chunk_id       TEXT    NOT NULL REFERENCES document_chunks(id) ON DELETE CASCADE,
    model_key      TEXT    NOT NULL,
    dim            INTEGER NOT NULL,
    vector         BLOB    NOT NULL,
    created_at_utc TEXT    NOT NULL,
    PRIMARY KEY (chunk_id, model_key)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_chunk_vectors_model ON chunk_vectors(model_key);
