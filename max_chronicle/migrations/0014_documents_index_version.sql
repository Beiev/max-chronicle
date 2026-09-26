-- 0014: which parser and secret filter indexed each note (FR-10, FR-11).
--
-- A note indexed by an older parser or filter is indexed again on the next
-- sync (notes.NOTE_INDEX_VERSION), so an improved filter reaches notes that did
-- not change. 0013 stays as first written: databases built with it get the
-- column here.

ALTER TABLE documents ADD COLUMN index_version INTEGER NOT NULL DEFAULT 0;
