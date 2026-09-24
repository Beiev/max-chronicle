# Chronicle Schema
> Current: schema 10. The filename is retained for existing links.

## Purpose

`chronicle.db` is the canonical local database for durable events, snapshots, entities, ingest runs, and Mem0 sync state.

The core storage model:

- append-only events and snapshots
- generic entities and relations
- explicit ingest bookkeeping
- searchable text via `FTS5`

## Current Migration Level

Current operational database version:

- `PRAGMA user_version = 13`
- latest applied migration: `0013_documents.sql`
- next migration number: `0014`

Migration `0007` adds the missing hot-path indexes for recent-event reads, outbox scans, latest situation lookup, and latest snapshot lookup.
Migration `0008` (memory v3) adds `entity_aliases`, the facts/episodes layer (`facts`, `episodes`, `fact_transactions`, `facts_fts`), `llm_calls`, and `recall_outbox`.
Migration `0009` adds `event_embeddings` (float32 BLOB vectors) backing the hybrid `query_memory` recall.
Migration `0011` rebuilds `events_fts` (event text and why only; no longer the project and domain slugs in `title` and `circumstances`) and `facts_fts` (now contentful) with ё folded into е, because FTS5's unicode61 tokenizer keeps the two apart.
Migration `0012` replaces `event_embeddings` with `event_vectors`, keyed by (event, embedding model key), so the index of a new model is backfilled beside the old one; existing vectors keep their model name as key.
Migration `0013` adds the note index (FR-10): `documents`, `document_chunks`, `document_chunks_fts` and `chunk_vectors`.

## Core Tables

### `entities`

Stable subjects such as projects, systems, assets, people, or documents.

Key columns:

- `id`
- `entity_type`
- `slug`
- `name`
- `metadata_json`

### `events`

Append-only durable facts and decisions.

Key columns:

- `occurred_at_utc`
- `occurred_at_local`
- `timezone`
- `event_type`
- `category`
- `entity_id`
- `text`
- `why`
- `payload_json`

Rules:

- `events` are never rewritten for meaning
- later corrections should be new events
- `why` is required whenever a decision or constraint exists

### `snapshots`

Point-in-time captures of system state.

Key columns:

- `captured_at_utc`
- `captured_at_local`
- `domain`
- `title`
- `focus`
- `summary_text`
- `payload_json`

### `artifacts`

Metadata for raw evidence and archived inputs.

Examples:

- exported JSON states
- digests
- source snapshots
- future asset manifests and repo captures

### `relations`

Typed links between entities.

Examples:

- `depends_on`
- `blocks`
- `generated_from`
- `belongs_to`

### `ingest_runs`

Operational audit log for imports and adapters.

Why it exists:

- idempotency
- forensic debugging
- tracking data freshness and failures

### `mem0_outbox`

Queue for derived semantic sync into Mem0.

Rule:

- Chronicle first
- Mem0 later

## Memory V2 Derived Tables

Schema v1 now also carries derived analytical tables without changing the truth rule:

### `normalized_entities`

Canonical alias groups for companies, projects, systems, topics, and later people.

### `situation_models`

Read-only derived state models linked to a Chronicle snapshot.

### `lens_runs`

Evidence-linked fixed analytical lenses over one `situation_model`.

### `scenario_runs`

Conditional what-if branches with explicit assumptions and expected outcomes.

### `forecast_reviews`

Replay/eval records that compare scenario expectations to later truth.

## Search Layer

Schema v1 creates:

- `events_fts`
- `snapshots_fts`

This keeps semantic lookup local and inspectable even without live Mem0.

## Timeline View

`v_chronicle_timeline` combines events and snapshots into one ordered query surface.

It is a convenience view for:

- timeline reconstruction
- operator inspection
- MCP adapters later

## Invariants

1. Every durable record must have a stable timestamp.
2. UTC is canonical for ordering.
3. Local timestamp is stored for human reconstruction.
4. JSON payloads must pass `json_valid`.
5. Imports must be idempotent.
6. Mem0 sync failures must never corrupt canonical truth.
7. Derived analytical tables must never be treated as canonical truth.

## Transitional Import

Current import sources:

- `ssot-ledger.jsonl` → `events`
- `chronicle-snapshots.jsonl` → `snapshots`

This is bootstrap only.

Long-term writes should target Chronicle directly, not JSONL first.

## Note index (migration 0013)

`documents` holds one row per indexed note file, keyed by a hash of its path:
source, canonical project (NULL for a global note), title, description, kind,
content sha256, size, file mtime, and `deleted_at_utc` for a tombstone. A deleted
note keeps its row and loses its chunks. Documents have no task or domain.

`document_chunks` are a note's sections (`Title › Section`), at most 1,500
characters each, stored after the secret filter. `document_chunks_fts` indexes
heading and text with ё folded into е, like `events_fts`. `chunk_vectors` are
keyed by (chunk, embedding model key), like `event_vectors`, and cascade with
their chunk.

The files stay the source: `chronicle notes sync` re-indexes changed notes,
tombstones deleted ones, and never writes to a note.

## Shared agent memory (migration 0010)

`event_observations` retains each independent author/session/task observation of
an event, its evidence receipt, and optional checkpoint. A monotonic `seq` powers
scope-bound continuation cursors; old events are backfilled without changing IDs.
`request_id` is unique and bound to request content. A reconnect may change the
transport session while replaying the same committed request.

Explicit assertions use the existing `episodes`, `facts`, `fact_observations`,
`fact_transactions`, `fact_supersessions`, and `fact_mutation_log` tables. A fact
slot belongs to domain/project/task scope. Replacements require the current ID;
the old row is retired and linked to its replacement. `current_facts` is a view,
not a separately rewritten truth store. `attributes_json.kind` distinguishes
observations, decisions, and assumptions.

Events, observations, fact mutations, and artifact links share one transaction.
Lexical/vector recall excludes superseded fact events, while historical reads
retain them. Legacy analytical tables are retained for historical compatibility;
the current tool surface does not produce speculative scenarios.
