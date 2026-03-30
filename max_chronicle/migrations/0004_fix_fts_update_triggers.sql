DROP TRIGGER IF EXISTS events_ad;
DROP TRIGGER IF EXISTS events_au;
DROP TRIGGER IF EXISTS snapshots_ad;
DROP TRIGGER IF EXISTS snapshots_au;

CREATE TRIGGER events_ad AFTER DELETE ON events BEGIN
    DELETE FROM events_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER events_au AFTER UPDATE ON events BEGIN
    DELETE FROM events_fts WHERE rowid = old.rowid;
    INSERT INTO events_fts(rowid, event_id, title, text, why, circumstances)
    VALUES (
        new.rowid,
        new.id,
        COALESCE(new.title, ''),
        new.text,
        COALESCE(new.why, ''),
        COALESCE(new.circumstances, '')
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
