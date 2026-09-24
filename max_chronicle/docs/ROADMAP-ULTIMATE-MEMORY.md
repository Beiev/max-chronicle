# Shared Agent Memory Specification

## Metadata

**Author:** Codex (v1.0), Claude (v1.1)
**Date:** 2026-09-15; v1.1 2026-09-23
**Status:** v1.0 implemented and validated; v1.1 accepted, in progress (see
[Implementation status](#implementation-status))
- Reviewers: project maintainer (approved the review proposals and the v1.1 direction)

## Context

Chronicle preserves events, but a new agent must also recover the right task
context and distinguish current decisions from older assertions. Search must
not substitute unrelated recent activity for evidence. Independent confirmations
must survive deduplication, and backups must prove evidence is restorable.

Keep SQLite, CLI, and MCP. Keep private workspaces outside the public package.
Existing entrypoints remain compatible; new fields are additive.

v1.1 follows a review of a live deployment. Its memory sat outside the agents'
loop: agents seldom wrote to it and seldom read it, while the notes they did keep
lived in files other agents could not search. Recall also failed on its main
language: Russian questions in natural phrasing, and any word with "ё", found
nothing. v1.1 therefore pushes a small brief into every session, indexes
explicitly written notes, fixes lexical recall for Cyrillic and natural phrasing,
and measures retrieval before changing it.

## Functional Requirements

- FR-1: Recall MUST apply domain, project, task, quarantine, model, and dimension
  constraints before ranking. Quarantine (`raw_only`) is the only visibility
  rule: every registered agent reads the same memory. Recency MUST only reorder
  relevant candidates and MUST NOT outweigh relevance (v1.1: it breaks ties).
- FR-2: Recall MUST return no results when no eligible evidence matches and MUST
  disclose unavailable or incomplete vector coverage. v1.1: lexical recall first
  requires every query term; when that finds nothing it MAY retry with any term or
  prefix and MUST then flag the response `relaxed`. A response without confident
  evidence MUST say `no_confident_match` rather than look like an answer.
  Tokenization MUST keep every letter of the supported languages inside its word,
  including Cyrillic ё, і, ї, є, ґ.
- FR-3: Writes MUST preserve independent observations, actor/session/task identity,
  and evidence. Request IDs MUST make retries idempotent and reject changed input,
  compared after redaction (FR-11).
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
- FR-9: Opening an existing database MUST NOT change its schema. An upgrade runs
  only by an explicit command, a server started with `--migrate`, or a CLI or
  library process that opts in with `CHRONICLE_AUTO_MIGRATE`; always under the
  write lock and after an online backup. A missing database is initialised on
  first use by a process that may write. A database newer than the code, another
  application's SQLite file, and duplicate migration numbers MUST be refused. A
  read-only server opens every connection read-only: it never creates,
  initialises, upgrades, or writes a database. Health MUST report the database
  path and schema version.
- FR-10: Capture stays explicit: agents record events, checkpoints, and facts on
  purpose. Chronicle MAY index notes someone wrote on purpose (such as an agent's
  file memory) as documents and MUST NOT extract memories from transcripts with a
  model. The files stay the source: the index is rebuilt from them, never writes
  to them, and keeps a tombstone for a deleted note. A note inside a project root
  belongs to that project; any other note is global and ranks below project notes
  in a project-scoped query. Documents have no task or domain.
- FR-11: A content-based secret filter MUST run before anything is stored,
  indexed, or archived: event text, evidence files, generated text artifacts,
  snapshots, imports, and indexed notes. It redacts
  likely credentials (known key prefixes, key/token/secret assignments,
  high-entropy strings, but not commit hashes or UUIDs) and reports how many it
  redacted. A secret MUST NOT reach derived output such as outboxes, dumps, or
  briefs.
- FR-12: `GET /brief` MUST return a read-only startup brief of at most 8,000
  characters: open checkpoints (the latest checkpoint of each task in scope whose
  `task.status` fact is not `completed` or `cancelled`), current facts, recent decisions with their ids,
  freshness and degradation warnings, and a three-line protocol, introduced as
  data rather than instructions. It MUST refuse any request that carries an
  `Origin` header or a host outside the loopback allowlist, and it MUST read
  through a `query_only` connection.
- FR-13: Retrieval changes MUST be measured. `chronicle eval` scores a golden set
  by hit@k, MRR@10, abstention, knowledge-update order, and latency; the public
  suite pins which synthetic cases lexical recall passes. Private golden sets and
  their reports stay outside the package.
- FR-14: Agent, project (with aliases and roots), task, and domain names MUST
  come from one registry, and every filter MUST resolve aliases through one scope function.
  Stored history is not rewritten; writes keep the raw actor next to the
  canonical one, and cursors issued before the registry keep working.

## Non-Functional Requirements

- NFR-1: Core workflows MUST work without Ollama, Mem0, or paid calls; Python 3.11+
  remains supported. New required external services: zero.
- NFR-2: Existing entrypoints MUST remain available. Migration MUST retain all old
  events and pass foreign-key checks with zero violations.
- NFR-3: Public tests MUST use synthetic temporary workspaces. Task context MUST
  be bounded to the requested item limit (1–100); history is explicitly paginated.
- NFR-4: The brief MUST answer within 300 ms at p95. A session MUST start normally
  when the server is down: hooks give up within 2 seconds, silently.

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
### AC-12: (FR-2)

Given evidence written with "ё" or in Ukrainian and a question in
  natural phrasing, when recalling, then the evidence is found; a relaxed match
  says so; a question memory cannot answer returns `no_confident_match`.
### AC-13: (FR-9)

Given a database behind the code, when a CLI or read-only server opens it,
  then nothing changes and the caller is told to migrate; `--migrate` backs it up,
  then upgrades it. Two concurrent upgraders produce one upgrade and one backup.
### AC-14: (FR-10, FR-11)

Given a note written today containing a synthetic key, when the
  index syncs, then another agent finds the note within one sync interval and no
  stored text contains the key.
### AC-15: (FR-12, NFR-4)

Given a running server, when a session starts in a project directory,
  then it receives a brief under 8,000 characters with that project's open
  checkpoint; a request with a foreign `Origin` gets 403; a stopped server
  delays nothing.
### AC-16: (FR-13)

Given a search change, when the suite runs, then any synthetic case it
  breaks fails the build, and any case it fixes must be added to the pinned set.
### AC-17: (FR-14)

Given events written under several spellings of one agent, project, task,
  or domain, when filtering by any spelling, then all of them are found.
### AC-18: (FR-11)

Given an event whose text and evidence file hold a synthetic key, when it
  is recorded, then no stored row, index, outbox, ledger, or archived copy holds
  the key, the receipt counts the redaction, and redaction time stays linear in
  the input.

## Edge Cases

- EC-1: Ollama unavailable, empty index, wrong model/dimension, invalid vectors.
- EC-2: No matching knowledge, competing assertions, stale revision, empty task.
- EC-3: Concurrent retries, duplicate prose from a new agent, missing files.
- EC-4: Unmounted target, corrupt artifacts, relocated backup, legacy backups.
- EC-5: Projection/compatibility failure after a canonical commit.

## API Contracts

MCP uses `POST /mcp`; liveness uses `GET /health`, which also reports
`db_path`, `schema_version`, and `target_schema_version` (FR-9). The brief is
`GET /brief?cwd=&project=&budget=&format=` (FR-12). Existing tools remain. Startup and recall gain optional `project` and `task_id`;
startup also gains `since`. Recording gains optional `request_id`, `session_id`,
`task_id`, `checkpoint`, and `fact`. Validation uses the existing error envelope.

The CLI and the server exit with 2 when the workspace cannot be resolved (see
`CHRONICLE_REQUIRE_ROOT`) and with 3 when the schema needs action (FR-9). `chronicle eval --golden FILE` reads one JSON case per
line and exits 1 when a `--fail-under METRIC=FLOOR` is missed (FR-13):

```typescript
interface GoldenCase {
  id: string; query: string;
  category: "fact" | "rationale" | "knowledge_update" | "temporal" | "handoff" | "abstention";
  expected: string[];  // "event:<id>"; empty only for abstention
  stale?: string[];    // superseded evidence that must rank below `expected`
  lang?: string; scope?: {domain?: string; project?: string; task_id?: string};
  notes?: string;
};
```

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
| Document | Indexed note (FR-10): source path, content hash, project or global, tombstone; chunks carry document and section headings |
| Embedding | Planned: keyed by object and model key; the key names the model and its prompt prefixes, so two indexes can coexist during a model change (today one vector per event) |

## Out of Scope

- OS-1: Automatic collection from all chats/apps, and model-based extraction of
  memories from transcripts. Capture stays explicit; connectors can adopt the
  protocol without indiscriminate personal data ingestion. Indexing notes someone
  wrote on purpose (FR-10) is in scope.
- OS-2: New orchestration platform, UI, or vector database.
- OS-3: Automatically guessing truth or resolving conflicting human decisions.
- OS-4: Publishing private configuration, credentials, or runtime history.
- OS-5: Per-agent visibility tiers. Registered agents share one memory; quarantine
  (FR-1) and the secret filter (FR-11) are the only barriers.

## Implementation status

| Requirement | Status |
| --- | --- |
| FR-1 to FR-8, NFR-1 to NFR-3 | Implemented in 0.10.0. Recency only breaks ties since 0.12.0 (FR-1 v1.1). |
| FR-2 v1.1 | Unicode tokens, ё folding, flagged relaxed match: 0.11.0. `no_confident_match`: 0.12.0 |
| FR-9, FR-13 | 0.11.0 |
| FR-11 | Events, evidence files, generated text artifacts: 0.11.0. Notes: 0.14.0. Snapshots, imports, Mem0 responses: planned |
| FR-10 | 0.14.0: note index (`[notes]` in the manifest, `chronicle notes sync`, `notes` in `query_memory`, `chronicle://note/{id}`) |
| FR-12, NFR-4 | Brief (`GET /brief`, `chronicle brief`, `startup_bundle(mode="brief")`, `chronicle://brief/{project}`): 0.12.0; p95 12 ms on a live-size database. Session hooks are operator configuration. |
| FR-14 | 0.13.0: registry in the manifest (`[[projects]]`, `[[domains]]`, `[[agents]]` with aliases); one scope function for events, observations, facts, open tasks, the brief and cursors |
