# ADR-0001 — Chronicle As Canonical Memory
> Status: accepted
> Date: 2026-03-15

## Context

The workspace already has useful memory layers:

- `status/*.md` for current state and operating rules
- `mem0-dump.json` and live Mem0 for semantic recall
- `ssot-ledger.jsonl` for durable notes
- `chronicle-snapshots.jsonl` for timestamped snapshots

That stack is good enough for migration, but it is not yet a clean long-term system of record.

Current problems:

1. Truth is split across markdown, JSONL, and Mem0.
2. Mem0 is good at recall, but weak as an authoritative temporal database.
3. JSONL is inspectable, but weak for idempotent ingestion, indexing, and precise time-travel queries.
4. Future agents need one stable contract, not a scavenger hunt across files and conventions.

## Decision

Adopt `Chronicle` as the canonical local-first memory system.

The architecture is:

- `chronicle.db` on SQLite = canonical truth
- `chronicle-artifacts/` = raw historical inputs and evidence
- `status/*.md` = readable projections and operator-facing views
- `Mem0/Qdrant` = derived semantic recall layer
- `MCP Chronicle` = universal access layer for future agents

### Core rules

1. Durable facts are written to Chronicle first.
2. Mem0 is a replica/index, not the source of truth.
3. `status/*.md` remain important, but as curated views and runbooks.
4. Time-based reconstruction prefers timestamped Chronicle data over later summaries.

## Why SQLite

SQLite is the right default because it is:

- local-first
- inspectable
- low-ops
- stable over many years
- strong on temporal querying and indexing
- good enough for one primary writer with many readers

Required SQLite features for this system:

- `WAL`
- `STRICT` tables
- `FTS5`
- JSON validation/functions
- simple backup/restore

## Consequences

Positive:

- one canonical timeline
- stable append-only history for decisions and blockers
- exact timestamp reconstruction becomes practical
- future MCP integrations can expose a single interface

Trade-offs:

- a migration layer is needed while JSONL and markdown remain active
- agents must follow a stricter write contract
- raw artifacts need deliberate storage instead of relying on live repo state

## Rejected Alternatives

### Mem0 as SSOT

Rejected because semantic memory is not a reliable temporal database and write reliability is already weaker than the required standard.

### JSONL as permanent truth layer

Rejected because it does not provide relational integrity, robust indexing, idempotent imports, or durable query ergonomics.

### Postgres now

Rejected for now because it raises operational complexity before the system has proven multi-writer or remote requirements.

### DuckDB as operational store

Rejected because the primary need is durable operational history, not analytics-first OLAP workflows.

## Migration Plan

Phase 1:

- create the Python package and SQLite schema
- import legacy ledger/snapshot data
- keep `ssot_hub.py` as the operational activation layer

Phase 2:

- write durable facts to Chronicle first
- generate markdown projections from Chronicle
- add outbox sync into Mem0

Phase 3:

- expose Chronicle via MCP
- archive raw artifacts and source snapshots
- move activation/timeline queries to the database-backed contract
