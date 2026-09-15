# Shared Agent Memory Specification

## Metadata

**Author:** Codex
**Date:** 2026-09-15
**Status:** Implemented and validated
- Reviewers: project maintainer (approved the review proposals)

## Context

Chronicle preserves events, but a new agent must also recover the right task
context and distinguish current decisions from older assertions. Search must
not substitute unrelated recent activity for evidence. Independent confirmations
must survive deduplication, and backups must prove evidence is restorable.

Keep SQLite, CLI, and MCP. Keep private workspaces outside the public package.
Existing entrypoints remain compatible; new fields are additive.

## Functional Requirements

- FR-1: Recall MUST apply domain, project, task, visibility, model, and dimension
  constraints before ranking. Recency MUST only reorder relevant candidates.
- FR-2: Recall MUST return no results when no eligible evidence matches and MUST
  disclose unavailable or incomplete vector coverage.
- FR-3: Writes MUST preserve independent observations, actor/session/task identity,
  and evidence. Request IDs MUST make retries idempotent and reject changed input.
- FR-4: Receipts MUST distinguish durable event success from missing/pointer-only
  evidence and failed derived output.
- FR-5: Startup MUST accept project/task/focus and return relevant evidence,
  current facts, the latest checkpoint, changes since a cursor, and a next cursor.
  Checkpoints MUST retain goal, completed work, verification, unknowns, and next steps.
- FR-6: Explicit facts MUST retain provenance and replacement history. Replacing
  a fact MUST name the expected current revision; stale updates MUST fail.
  Assumptions MUST remain distinguishable from observed facts and decisions.
- FR-7: Durable writes MUST use full SQLite synchronization. Restore checks MUST
  verify archived content hashes and work without the original source workspace.
- FR-8: Agent docs MUST describe the implemented contract. Built distributions MUST
  exclude personal data. Local health MUST distinguish availability from data health.

## Non-Functional Requirements

- NFR-1: Core workflows MUST work without Ollama, Mem0, or paid calls; Python 3.11+
  remains supported. New required external services: zero.
- NFR-2: Existing entrypoints MUST remain available. Migration MUST retain all old
  events and pass foreign-key checks with zero violations.
- NFR-3: Public tests MUST use synthetic temporary workspaces. Task context MUST
  be bounded to the requested item limit (1–100); history is explicitly paginated.

## Acceptance Criteria

### AC-1: (FR-1, FR-2)

Given unrelated/hidden records, when querying another topic
  or scope, then none appear merely because they are recent or vector-indexed.
### AC-2: (FR-1, FR-2, NFR-1)

Given missing/incompatible vectors, when searching,
  then lexical recall works and vector degradation is explained.
### AC-3: (FR-3)

Given two agents confirming one decision with different evidence,
  when both record it, then both observations and attachments remain retrievable.
### AC-4: (FR-3)

Given a committed request ID, when retried, then no duplicate is
  created; changed content under that ID is rejected.
### AC-5: (FR-4)

Given missing evidence or a failed compatibility write, when
  recording, then the receipt distinguishes durable memory from incomplete output.
### AC-6: (FR-5, NFR-3)

Given separate tasks, when starting each task, then its
  context/checkpoint/cursor are scoped correctly and changes can be paginated.
### AC-7: (FR-5)

Given a checkpoint by agent A, when agent B starts without chat
  history, then B recovers goal, verified results, unknowns, and next steps.
### AC-8: (FR-6)

Given an active fact, when explicitly replaced, then current recall
  returns its replacement, history retains both, and stale replacement is rejected.
### AC-9: (FR-7)

Given altered/missing backup evidence, when restoring, then validation
  fails; an intact relocated copy passes without the original workspace.
### AC-10: (FR-8, NFR-2)

Given a built package in a clean environment, when initialized
  and used through CLI/MCP, then the documented workflow works without personal paths.
### AC-11: (NFR-1, NFR-2)

Given the old suite and an old database, when upgrading,
  then compatibility tests and migration integrity checks pass.

## Edge Cases

- EC-1: Ollama unavailable, empty index, wrong model/dimension, invalid vectors.
- EC-2: No matching knowledge, competing assertions, stale revision, empty task.
- EC-3: Concurrent retries, duplicate prose from a new agent, missing files.
- EC-4: Unmounted target, corrupt artifacts, relocated backup, legacy backups.
- EC-5: Projection/compatibility failure after a canonical commit.

## API Contracts

MCP uses `POST /mcp`; liveness uses `GET /health`. Existing tools remain. Startup and recall gain optional `project` and `task_id`;
startup also gains `since`. Recording gains optional `request_id`, `session_id`,
`task_id`, `checkpoint`, and `fact`. Validation uses the existing error envelope.

```typescript
interface Checkpoint {
  goal: string; completed: string[]; verification: string[];
  open_questions: string[]; next_steps: string[];
};
interface Fact {
  slot: string; value: string;
  kind: "observed" | "decision" | "assumption";
  supersedes?: string;
};
interface Receipt {
  id: string; chronicle_status: "stored" | "existing";
  observation_id?: string; fact_id?: string;
  evidence?: {path: string; status: string}[];
};
```

## Data Models

| Model | Fields and constraints |
| --- | --- |
| Observation | UUID, event FK, optional unique request ID, request hash, actor, session/task/project, time, immutable payload |
| Checkpoint | Validated observation payload; requires project and task identity |
| Fact | Existing facts/episodes/observations/transactions/supersessions tables; scoped explicit replacement |
| Cursor | Opaque continuation position bound to domain/project/task; includes new observations of old events |
| Artifact | Existing hash and link; per-file receipt and verified backup inventory |

## Out of Scope

- OS-1: Automatic collection from all chats/apps. Capture stays explicit; connectors
  can adopt the protocol without indiscriminate personal data ingestion.
- OS-2: New orchestration platform, UI, or vector database.
- OS-3: Automatically guessing truth or resolving conflicting human decisions.
- OS-4: Publishing private configuration, credentials, or runtime history.
