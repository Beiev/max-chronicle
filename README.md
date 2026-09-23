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
   evidence and `degraded`; empty results mean no suitable evidence was found.
3. **Record:** use `record_event` for significant decisions, actions, and findings.
   Supply `why` and `source_files` where available. Reuse `request_id` unchanged
   if delivery is uncertain; a changed request requires a new ID. Likely secrets
   (API keys, tokens, passwords) are redacted before anything is stored, in the
   event and in archived evidence files; the receipt's `redactions` counts them.
4. **Hand off:** attach a `checkpoint` to `record_event` before a context switch.
   Record what was actually verified and what remains unknown.
5. **Resume:** the next agent reads `task_context.checkpoint`, `current_facts`, and
   `changes`. Save `task_context.cursor`; pass it as `since` on the next startup.
   If `has_more` is true, continue paging with the returned cursor.

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
`chronicle-mcp --transport streamable-http`; `MCP_HOST`/`MCP_PORT` set its address.
`GET /health` checks service and database availability; `sources_audit` and startup
source health describe freshness. These are different checks.

| Tool | Purpose |
| --- | --- |
| `startup_bundle` | Task context, checkpoint, current facts, change cursor; unlock writes. |
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

- **Lexical:** SQLite FTS5/BM25 over event text and explicit current fact values.
- **Vector:** optional Ollama `nomic-embed-text` index. Model, dimension, scope,
  and visibility are checked before ranking. `vector_coverage` discloses gaps;
  `chronicle embed-backfill` repairs missing/incompatible index rows.
  If native scheduling is enabled, daily capture also retries up to 10 missing
  or incompatible embeddings per run. Disabling event embeddings disables this repair.
- **Recency:** reorders relevant candidates; it never supplies unrelated answers.
- **Threshold:** `CHRONICLE_VECTOR_MIN_SIMILARITY` defaults to `0.65`. Evaluate it
  on your own corpus; cosine and reciprocal-rank scores are not confidence values.
- **Writes:** SQLite WAL with full commit synchronization; events, observations,
  facts, and evidence links commit together.
- **Secrets:** a content-based filter replaces likely credentials with
  `[REDACTED:<kind>]` before an event, its checkpoint or fact, or a UTF-8 evidence
  file is stored: known key prefixes, values assigned to secret names, bearer
  tokens, URL passwords, private key blocks, and high-entropy tokens on a line
  that mentions a key, token, or password. Hex digests and UUIDs are never treated
  as secrets. Identifiers such as `request_id` and paths are left as given, the
  source file is never modified, and binary evidence is archived unchanged.
- **Backups:** independent artifact copies, content-hash inventory, database
  integrity checks, and verification after relocation. Legacy backups disclose
  incomplete inventory coverage. Choose a separate backup device for disk failure.
  Explicitly purged artifacts are counted separately; they are not claimed as restorable.

## CLI

```bash
chronicle record "Selected local storage" --project demo --task-id ship \
  --agent agent-a --request-id decision-1 --category decision --why "Works offline"
chronicle record "Ready for review" --project demo --task-id ship \
  --checkpoint-file checkpoint.json --request-id handoff-1
chronicle startup --project demo --task-id ship --focus "Continue review" --format json
chronicle query-memory "local storage" --project demo --task-id ship --format json
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
