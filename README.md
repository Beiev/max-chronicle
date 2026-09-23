# Max Chronicle

[![tests](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml/badge.svg)](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml)
[![python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Local-first shared memory for AI agents.** Preserve decisions, evidence, and
working state so another agent can continue a task without the previous chat.

Chronicle stores attributed events and current assertions in SQLite. Agents use
MCP or the CLI to search, record observations, and exchange checkpoints. Local
vector search is optional; the core works without Ollama, Mem0, or paid APIs.

A stored assertion is not automatically a verified fact. Its author, evidence,
and kind (`observed`, `decision`, or `assumption`) remain visible.

## Quick start

```bash
git clone https://github.com/Beiev/max-chronicle.git
cd max-chronicle
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
chronicle init --root ~/chronicle-workspace
export CHRONICLE_ROOT=~/chronicle-workspace
chronicle startup --project demo --task-id ship --format json
```

Keep the workspace separate from the source checkout. Initialization creates
neutral configuration and starter documents, with no personal data or invented
projects. See [installation](INSTALL.md).

## Agent protocol

Use the same `project` and `task_id` across agents. Domains are broad configured
areas; they are not a substitute for project/task identity.

1. **Start:** call `startup_bundle(project=..., task_id=..., focus=...)` once per
   MCP session. It returns the current task context and unlocks writes.
2. **Recall:** call `query_memory(query=..., project=..., task_id=...)`. Inspect
   evidence, `degraded` and `no_confident_match`. Results without a confident match
   are leads to verify; a weak or empty result does not prove the memory is absent.
3. **Record:** use `record_event` for significant decisions, actions, and findings.
   Supply `why` and `source_files` where available. Reuse `request_id` unchanged
   if delivery is uncertain; a changed request requires a new ID. Likely secrets
   (API keys, tokens, passwords) are redacted before anything is stored, in the
   event and in archived evidence files; the receipt's `redactions` counts them.
4. **Hand off:** attach a `checkpoint` to `record_event` before a context switch.
   Record what was actually verified and what remains unknown.
5. **Resume:** the next agent reads `task_context.checkpoint`, `current_facts`, and
   `changes`. Save `task_context.cursor`; pass it as `since` on the next startup.
   If `has_more` is true, continue paging with the returned cursor. For older
   changes, pass `task_context.before` as `before` while `has_older` is true.
   Only a named task has a checkpoint; a startup without `task_id` lists
   `open_tasks` instead (the latest checkpoint of each task in scope whose
   `task.status` fact is not `completed` or `cancelled`), so pick one and start again
   with its `project` and `task_id`.

Task startup includes current facts from the project and the selected task.
Their scope stays explicit; another task's facts are excluded. If `facts_has_more`
is true, increase `limit` (up to 100) or use scoped recall to find a specific slot.

### Example handoff

MCP `record_event` arguments:

```json
{
  "text": "Offline recall implemented; vector evaluation remains",
  "why": "The next agent can continue from the verified lexical path",
  "project": "demo",
  "task_id": "ship",
  "request_id": "demo-ship-checkpoint-1",
  "checkpoint": {
    "goal": "Ship reliable recall",
    "completed": ["Implemented lexical search"],
    "verification": ["Offline search test passed"],
    "open_questions": ["Which vector threshold works for this corpus?"],
    "next_steps": ["Evaluate vector precision on representative queries"]
  }
}
```

MCP assigns a session identity automatically. Independent confirmations preserve
both authors and attachments even when the underlying event text is deduplicated.
`chronicle_status` acknowledges the event; `observation_id` identifies this
observation. `evidence` reports each attachment as archived, pointer-only, missing,
or skipped. A pointer is not an archived copy.

### Current knowledge and corrections

Supply an explicit `fact` object on `record_event`:

```json
{"slot": "project.status", "value": "active", "kind": "observed"}
```

The receipt includes `fact_id`. To change that scoped slot, include its current
ID in the next assertion:

```json
{"slot": "project.status", "value": "paused", "kind": "decision", "supersedes": "<current-fact-id>"}
```

Chronicle retains the old assertion and its evidence. Stale replacements fail
with an actionable error. Current recall excludes superseded assertions; historical
queries retain the event trail. Unstructured prose is never automatically promoted
into a fact, and assumptions are not silently converted into verified knowledge.

## MCP integration

Start `chronicle-mcp-chronicler` as a direct child process of the client. A typical
MCP configuration uses an absolute installed command and workspace path:

```json
{
  "mcpServers": {
    "chronicle": {
      "command": "/absolute/path/to/.venv/bin/chronicle-mcp-chronicler",
      "env": {"CHRONICLE_ROOT": "/absolute/path/to/chronicle-workspace"}
    }
  }
}
```

Use your client's equivalent configuration format. Stdio starts with the client
and does not require a separately running HTTP service. For supervised HTTP, use
`chronicle-mcp --transport streamable-http --migrate`; `MCP_HOST`/`MCP_PORT` set its address.
`GET /health` checks service and database availability and names the database,
manifest, schema version and installed code path that answered; `sources_audit` and
startup source health describe freshness. These are different checks.

### Session brief

`GET /brief` returns a compact read-only brief (FR-12) that a new session reads
first:
- open tasks with their latest checkpoint;
- current facts;
- decisions from the last 14 days, each with its id;
- freshness and failed-job warnings;
- a three-line protocol.

It opens by saying it is data, not instructions. The brief reads through a
`query_only` connection and answers in milliseconds. It refuses any request that
carries an `Origin` header or a `Host` outside loopback, so a web page cannot
read it. Parameters:
- `cwd`: the working directory;
- `project`: a project slug that overrides `cwd`;
- `budget`: 1,000 to 8,000 characters, 6,000 by default;
- `format=json`: return JSON instead of plain text.

The project comes from `project`, from a configured root that holds `cwd`, or
from the name of `cwd` itself when that name is a known project. Parent
directories are not searched, so pass a repository's top level:

```toml
# SSOT_MANIFEST.toml
[[projects]]
id = "atlas"
roots = ["~/code/atlas", "~/code/atlas-worktrees"]
```

A session-start hook prints the brief and stays silent when the server is down:

```sh
dir=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
curl -fsS -m 2 --get --data-urlencode "cwd=$dir" http://127.0.0.1:8093/brief 2>/dev/null || true
```

Claude Code and Codex add a SessionStart hook's plain standard output to the
session context; Claude Code keeps up to 10,000 characters, above the brief's
maximum. Without HTTP, `chronicle brief --cwd DIR` prints the same text. An MCP client can call `startup_bundle(mode="brief")`, which also unlocks
writes, or read the resource `chronicle://brief/{project}`.

| Tool | Purpose |
| --- | --- |
| `startup_bundle` | Task context, checkpoint, current facts, change cursor; unlock writes. `mode="brief"`: only the brief. |
| `query_memory` | Scoped lexical/vector recall with provenance and coverage. |
| `query_context` | Broader search through source documents and optional Mem0 dump. |
| `recent_events` | Recent event history. |
| `state_at` | Historical snapshots/events; `detail="full"` restores the full payload. |
| `sources_audit` | Source coverage, freshness, and trust metadata. |
| `record_event` | Attributed observation, evidence, optional checkpoint or explicit fact. |
| `capture_snapshot` | Archive runtime state and update readable projections. |
| `entity_admin` | Report, normalize, alias, or merge entities. |
| `search_mem0_live` | Optional external semantic mirror through an operator-supplied bridge. |

`chronicle-mcp-readonly` exposes only read surfaces. The deprecated `activate_agent`
alias remains compatible. Failures set MCP `isError` and carry a JSON envelope
with `error_type`, `retryable`, and `hint`; follow that hint. A committed snapshot
can separately report `side_effect_errors` for failed evidence/projection outputs.

## Recall and durability

- **Lexical:** SQLite FTS5/BM25 over event text and why, and over explicit
  current fact values. Queries drop function words (English, Russian,
  Ukrainian) but keep negations, keep every Unicode letter, and fold ё into е on
  both sides. Evidence must contain every remaining term; when no event or fact
  does, recall retries with word stems, keeps only events holding two thirds of
  the terms, and returns `relaxed: true` because that match is weaker.
- **Vector:** optional local index built through Ollama. The default model is
  `qwen3-embedding:0.6b` (multilingual; `ollama pull qwen3-embedding:0.6b`).
  `CHRONICLE_EMBED_MODEL` selects another; `nomic-embed-text` keeps an index
  built before 0.12 usable. Every vector is stored under its model, so a new
  model is backfilled beside the old index and recall reads only the active one.
  Queries carry the model's retrieval instruction, and an event longer than the
  model's context is embedded from its start instead of failing. Scope and
  visibility are checked before ranking. `vector_coverage` discloses gaps, and
  an active model with no index at all degrades the channel
  (`embedding_index_empty`). `chronicle embed-backfill` fills the active index.
  If native scheduling is enabled, daily capture also retries up to 10 missing
  or incompatible embeddings per run. Disabling event embeddings disables this repair.
- **Recency:** breaks ties between equally relevant candidates; it never supplies
  or promotes unrelated answers.
- **Confidence:** `no_confident_match: true`, with a `hint`, when no result holds
  every query term or reaches the model's confident similarity (`0.6` for
  `qwen3-embedding:0.6b`; none for `nomic-embed-text`, whose similarity does not
  tell related from unrelated text). The results are still returned as leads.
  `CHRONICLE_VECTOR_CONFIDENT_SIMILARITY` overrides the floor.
- **Threshold:** a candidate found only by the vector channel needs a cosine
  similarity of at least the model's floor: `0.5` for Qwen3, `0.65` for
  `nomic-embed-text`. `CHRONICLE_VECTOR_MIN_SIMILARITY` overrides it. Evaluate
  it on your own corpus with `chronicle eval`; cosine and reciprocal-rank scores
  are not confidence values.
- **Writes:** SQLite WAL with full commit synchronization; events, observations,
  facts, and evidence links commit together.
- **Schema changes:** an empty database initialises on first use. An existing one is
  never upgraded as a side effect of connecting: run `chronicle migrate` or start the
  server with `chronicle-mcp --migrate`; both take an online backup
  (`chronicle.db.bak-premigrate-…`) under the same write lock as the upgrade. Without
  `--migrate` a server refuses to start on an outdated schema (exit code 3), and a
  read-only server never creates or changes a database. A database newer than the installed code, or
  another application's SQLite file, is refused. After installing a release with new
  migrations, migrate before scheduled jobs run: until then they exit with code 3.
  `CHRONICLE_AUTO_MIGRATE=1` restores upgrade-on-connect for the CLI and library;
  a server upgrades only with `--migrate`. `CHRONICLE_REQUIRE_ROOT=1`
  makes a process without `CHRONICLE_ROOT`/`CHRONICLE_MANIFEST` exit with code 2
  instead of falling back to `~/.max-chronicle`.
- **Read-only servers:** SQLite reads a WAL database through its `-wal` and `-shm`
  files, so even a read-only server creates them when they are missing. Next to a
  live writer they already exist. To serve a copy from a read-only directory,
  switch the copy to `PRAGMA journal_mode = DELETE` first.
- **Secrets:** a content-based filter replaces likely credentials with
  `[REDACTED:<kind>]` before an event (with its checkpoint and fact), a UTF-8
  evidence file, or a generated text artifact (daybook, commit summary, audit
  report) is stored. It catches known key prefixes, values assigned to secret
  names, labelled keys (`key: <random>`), bearer and basic credentials, URL
  passwords, private key blocks, and high-entropy tokens near a word such as
  token, password, secret, or API key (anywhere on a short line, within 256
  characters on a long one). Hex digests, UUIDs, and pieces of long base64 runs
  (encoded images) never count as high-entropy tokens.
  Identifiers such as `request_id` and paths are left as given, the source file is
  never modified, and binary evidence is archived unchanged. Not filtered yet:
  snapshot excerpts, legacy imports, and Mem0 responses. Time is linear in the
  input, so a hostile or huge text cannot stall the write path.
- **Backups:** independent artifact copies, content-hash inventory, database
  integrity checks, and verification after relocation. Legacy backups disclose
  incomplete inventory coverage. Choose a separate backup device for disk failure.
  Explicitly purged artifacts are counted separately; they are not claimed as restorable.

### Measuring recall

`chronicle eval --golden questions.jsonl` asks each question through recall and
scores the ranking: hit@1/5/10, recall@5 and MRR@10 on answerable questions,
abstention on questions memory cannot answer, stale-over-current ordering on
knowledge updates, and latency percentiles, overall and per category and language.

```json
{"id": "why-sqlite", "query": "why did we pick SQLite", "category": "rationale", "lang": "en", "expected": ["event:<id>"], "scope": {"project": "demo"}}
{"id": "db-now", "query": "current database", "category": "knowledge_update", "expected": ["event:<new>"], "stale": ["event:<old>"]}
{"id": "unknown", "query": "office wifi password", "category": "abstention", "expected": []}
```

Categories follow LongMemEval: `fact`, `rationale`, `knowledge_update`, `temporal`,
`handoff`, `abstention`. `--json` prints the full report, `--out` saves it, and
`--fail-under hit@5=0.6` exits 1 below a floor. The test suite runs a synthetic
bilingual set (`tests/fixtures/eval/`) and pins which cases lexical recall passes.

## CLI

```bash
chronicle record "Selected local storage" --project demo --task-id ship \
  --agent agent-a --request-id decision-1 --category decision --why "Works offline"
chronicle record "Ready for review" --project demo --task-id ship \
  --checkpoint-file checkpoint.json --request-id handoff-1
chronicle startup --project demo --task-id ship --focus "Continue review" --format json
chronicle query-memory "local storage" --project demo --task-id ship --format json
chronicle eval --golden questions.jsonl --out results/baseline.json
chronicle timeline --at "2026-09-15T12:00:00Z"
chronicle backup --force
```

`--fact-file` accepts the assertion JSON above. Startup defaults to compact source
metadata; `--full` includes full source content. Run `chronicle --help` for optional
automation and maintenance commands.

## Architecture and development

```text
Agents / CLI / MCP
        |
   service.py                 record + startup coordination
        |--- memory.py       observations, checkpoints, current assertions
        |--- recall.py       scoped lexical/vector ranking
        |--- store.py        SQLite transactions and archived evidence
        |
   SQLite (schema 10) + content-addressed files
        |--- Markdown projections
        |--- optional Mem0 mirror
```

[Agent development guide](AGENTS.md) · [Schema](max_chronicle/docs/SCHEMA_V1.md) ·
[Requirements](max_chronicle/docs/ROADMAP-ULTIMATE-MEMORY.md)

```bash
pip install -e '.[dev]'
python -m pytest -q
python -m build
```

Tests use synthetic temporary workspaces. The cross-agent tests launch separate
MCP clients and verify handoff without shared conversation history. Private
workspaces, logs, credentials, databases, and artifacts never belong in a release.

MIT licensed.
