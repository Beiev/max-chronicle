# Chronicle Schema v1
> Updated: 2026-03-30 with migration v7 operational state.

## Purpose

`chronicle.db` is the canonical local database for durable events, snapshots, entities, ingest runs, and Mem0 sync state.

Schema v1 is intentionally boring:

- append-only events and snapshots
- generic entities and relations
- explicit ingest bookkeeping
- searchable text via `FTS5`

## Current Migration Level

Current operational database version:

- `PRAGMA user_version = 7`
- latest applied migration: `0007_missing_indexes.sql`
- next migration number to reserve: `0008`

Migration `0007` adds the missing hot-path indexes for recent-event reads, outbox scans, latest situation lookup, and latest snapshot lookup.

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
