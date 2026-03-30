# Chronicle Implementation Checklist

## Stable Baseline

Status: finished on 2026-03-29

- [x] Chronicle works as a stable local-first baseline for future agents
- [x] `chronicle.db` is the canonical truth in practice
- [x] MCP access, snapshots, projections, hooks, artifacts, native automation, and Mem0 outbox work together
- [x] Hardening roadmap closed for the declared scope

Maintenance note:
- Chronicle is finished and in maintenance-only mode.
- Remaining unchecked items below are historical backlog only, not active delivery scope.

Historical planning document:
- `max_chronicle/docs/ROADMAP-ULTIMATE-MEMORY.md`

## Phase 1 — Foundation

Status: done

- [x] Create a dedicated Python package for Chronicle
- [x] Add `pyproject.toml`
- [x] Create `chronicle.db`
- [x] Add schema v1 migration
- [x] Import legacy `ssot-ledger.jsonl`
- [x] Import legacy `chronicle-snapshots.jsonl`
- [x] Verify idempotent re-import
- [x] Verify database-backed timeline query
- [x] Align protocols so Chronicle is the canonical truth layer

## Phase 2 — Direct Writes

Status: closed for finished scope

- [x] Add native Chronicle write commands for durable events
- [x] Add native Chronicle write commands for snapshots
- [x] Stop treating JSONL as the first write target
- [x] Store `why` and source refs by default
- [x] Add validation rules and normalization for event categories
- [x] Add exact-duplicate guard for direct durable writes

## Phase 2.5 — Retrieval Contract Hardening

Status: closed for finished scope

- [x] Make source trust tiers explicit in the manifest
- [x] Surface freshness warnings through attach/startup/query payloads
- [x] Prefer fresher evidence before lower-value textual matches
- [ ] Tighten entity and relation governance beyond project-level validation

## Phase 3 — Projections

Status: closed for finished scope

- [x] Generate `status.md` hybrid projections from Chronicle-backed state
- [x] Generate `job-search-status.md` from Chronicle-backed state
- [ ] Define which markdown files stay hand-authored vs generated
- [x] Add projection freshness metadata

## Phase 4 — Artifact Archive

Status: closed for finished scope

- [x] Create content-addressed artifact storage in `chronicle-artifacts/`
- [x] Capture raw source evidence from OpenClaw, Digest, SSOT sources, and portfolio manifests
- [x] Hash and register archived artifacts in the database
- [x] Link artifacts to events and snapshots

## Phase 5 — Mem0 Sync

Status: done

- [x] Move Mem0 writes behind `mem0_outbox`
- [x] Add retryable sync worker
- [x] Preserve sync errors without touching canonical truth
- [ ] Define dedupe rules for Chronicle -> Mem0 replication

## Phase 6 — MCP Layer

Status: closed for finished scope

- [x] Expose Chronicle as an MCP server
- [x] Add tools for `state_at`, `timeline`, `record_event`, `capture_snapshot`
- [x] Add resources for current state, project state, and world context
- [x] Add prompts for activation and reconstruction
- [x] Harden MCP tool self-description with parameter docs and a query mode enum
- [x] Document the startup gate, read-surface visibility, and filtered raw-only handling

## Operational Guard Rails

- Chronicle stays local-first.
- SQLite stays the default until there is a proven multi-writer need.
- Durable facts are recorded once in Chronicle, then projected outward.
- Mem0 stays a semantic layer, not the truth layer.
- MiniMax health is checked with a completion canary, not with quota-only endpoints.
