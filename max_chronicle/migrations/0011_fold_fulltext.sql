-- 0011: fold ё into е in the full-text indexes, and index prose only.
--
-- FTS5's unicode61 tokenizer keeps ё and е apart in every remove_diacritics
-- mode, while Russian writers use both spellings of one word, so "отчет" never
-- found "отчёт". Indexed text is folded here; queries are folded the same way
-- in code (lexical.fold). events_fts keeps `text` and `why` and drops `title`
-- and `circumstances`, the project and domain slugs: scope is an SQL filter,
-- and a matched slug made every event of a project or domain look relevant.
-- facts_fts becomes contentful (it indexed `facts` as external content), so
-- its folded copy needs no view over `facts`.

DROP TRIGGER IF EXISTS events_ai;
DROP TRIGGER IF EXISTS events_ad;
DROP TRIGGER IF EXISTS events_au;
DROP TABLE IF EXISTS events_fts;

CREATE VIRTUAL TABLE events_fts USING fts5(
    event_id UNINDEXED,
    text,
    why,
    tokenize = 'unicode61'
);

INSERT INTO events_fts(rowid, event_id, text, why)
SELECT
    rowid,
    id,
    replace(replace(text, 'ё', 'е'), 'Ё', 'Е'),
    replace(replace(COALESCE(why, ''), 'ё', 'е'), 'Ё', 'Е')
FROM events;

CREATE TRIGGER events_ai AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, event_id, text, why)
    VALUES (
        new.rowid,
        new.id,
        replace(replace(new.text, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(COALESCE(new.why, ''), 'ё', 'е'), 'Ё', 'Е')
    );
END;

CREATE TRIGGER events_ad AFTER DELETE ON events BEGIN
    DELETE FROM events_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER events_au AFTER UPDATE ON events BEGIN
    DELETE FROM events_fts WHERE rowid = old.rowid;
    INSERT INTO events_fts(rowid, event_id, text, why)
    VALUES (
        new.rowid,
        new.id,
        replace(replace(new.text, 'ё', 'е'), 'Ё', 'Е'),
        replace(replace(COALESCE(new.why, ''), 'ё', 'е'), 'Ё', 'Е')
    );
END;

DROP TRIGGER IF EXISTS facts_ai;
DROP TRIGGER IF EXISTS facts_ad;
DROP TRIGGER IF EXISTS facts_au;
DROP TABLE IF EXISTS facts_fts;

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
