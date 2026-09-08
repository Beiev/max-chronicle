# Max Chronicle

[![tests](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml/badge.svg)](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml)
[![python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Local-first durable memory for AI agents.

Max Chronicle is a Python package for multi-agent systems that need context to survive across sessions. It keeps canonical history in local SQLite, exposes that history through a CLI and MCP server, and can attach an optional Mem0 semantic recall layer. It is not a cloud service; the core system runs entirely on your machine.

Core capabilities include append-only events, point-in-time snapshots, hybrid local recall (BM25 + vectors + recency), normalized entities, situation models, fixed analytical lenses, and what-if scenario analysis.

## Why

Agents are good at short-lived reasoning and bad at durable memory. Between sessions, threads, and tool calls, they lose state, repeat work, and blur truth with generated summaries.

Max Chronicle fixes that by separating local truth from derived views: record durable events and snapshots into SQLite, project readable state into Markdown, and let the next agent resume from an explicit startup or activation bundle instead of starting from zero.

## Quick Start

Requirements:

- Python 3.11+
- `pip` or `uv`

Optional: `pip install -e ".[pretty]"` for richer `chronicle browse` output.

Install from the repository:

```bash
git clone https://github.com/Beiev/max-chronicle.git
cd max-chronicle
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Create a workspace and point Chronicle at it:

```bash
chronicle init --root ~/chronicle-workspace
export CHRONICLE_ROOT=~/chronicle-workspace
chronicle status
```

Optional smoke test:

```bash
chronicle startup --domain global --format json
```

## Core Concepts

| Layer | Role |
| --- | --- |
| `chronicle.db` | Canonical truth. Local SQLite, schema v9. Stores append-only events, point-in-time snapshots, normalized entities, situation models, analytical lens runs, and scenario runs. |
| Markdown projections | Readable operator-facing views derived from Chronicle. Useful for inspection, handoff, and versioned docs, but not the source of truth. |
| Mem0 (optional) | External semantic recall, reached through an operator-supplied bridge script. Derived and non-authoritative. |

In practice: `chronicle.db` is truth, Markdown is projection, and Mem0 is recall.

## CLI Commands

| Command | Purpose |
| --- | --- |
| `chronicle init --root <path>` | Create a fresh Chronicle workspace with database, manifest, automation config, and starter files. |
| `chronicle status` | Report database health, migration state, and table counts. |
| `chronicle record "text"` | Append a durable event to the truth store. |
| `chronicle recent` | Show recent durable events. |
| `chronicle timeline --at <iso-timestamp>` | Reconstruct nearby snapshots and events around a point in time. |
| `chronicle capture-runtime --domain <id>` | Capture a runtime snapshot and refresh derived state. |
| `chronicle startup --domain <id>` | Build a compact startup bundle for a new agent session. |
| `chronicle activate --domain <id>` | Build the activation contract and prompt for an agent session. |
| `chronicle query "text"` | Search Chronicle, active source docs, and optional Mem0 recall. |

Maintenance commands: `normalize-entities`, `embed-backfill`, `query-memory`, `browse`,
`audit`, `doctor`, `backup`, and `repair-stale-runs`. Run `chronicle --help` for the full list.

## MCP Server

Max Chronicle ships an MCP server for agent integration, over stdio or streamable HTTP.

- `chronicle-mcp-readonly` exposes read-only resources and tools.
- `chronicle-mcp-chronicler` exposes the full chronicler surface, including writes.
- `chronicle-mcp --profile readonly|chronicler --transport stdio|streamable-http` is the generic entrypoint.
  For HTTP, `MCP_HOST` and `MCP_PORT` select the bind address.

### Tool surface

Read tools work immediately; write tools open after one `startup_bundle` call per session.

| Tool | Purpose |
| --- | --- |
| `startup_bundle` | Session entry point: returns the startup brief and unlocks the write surface. |
| `query_memory` | Primary recall. RRF fusion over full-text, vector, and recency channels. |
| `query_context` | Broader sweep that also reads Markdown sources and an optional Mem0 dump. |
| `recent_events` | Cheap latest-N feed for situational awareness. |
| `state_at` | Reconstruct what was true around a timestamp. Snapshots come back digested; `detail="full"` returns the stored payload verbatim. |
| `sources_audit` | Source coverage, freshness, and trust metadata. |
| `record_event` | Write a durable event and archive linked source files. |
| `capture_snapshot` | Capture live runtime state and refresh Markdown projections. |
| `entity_admin` | Entity maintenance: `report`, `normalize`, `alias`, `merge`. |
| `search_mem0_live` | Optional live semantic search. Requires an operator-supplied Mem0 bridge script, which this package does not ship; without it the tool reports `degraded` instead of failing. |

`activate_agent` is still registered as a deprecated alias of `startup_bundle` so existing
agents keep working; new integrations should call `startup_bundle`.

**Replies are sized for the caller.** A tool answers with what the caller needs,
not with an echo of what it already holds. `state_at` and `activate_agent` digest
the snapshots they return — capture-time copies of the ledger and of semantic
recall are replaced by a count, because live recall serves that data better and
more currently — and `capture_snapshot` answers with a receipt (id, artifacts
written, projection hashes) rather than replaying the snapshot it just stored.
This matters because the calls an agent makes at session start and before a
handoff are the ones where its remaining context is scarcest. The CLI still
prints the unabridged payloads.

### Operational behaviour

- **Tool bodies run off the event loop.** Handlers execute in worker threads with
  separate read and write limiters, so one slow call (a git capture, an embedding
  request, a cold subprocess) cannot freeze other sessions.
- **Failures are unambiguous.** A failing tool raises, so the call is flagged
  `isError`, and the message carries a machine-readable envelope after the
  FastMCP prefix: `{"status": "error", "error_type", "retryable", "hint"}`.
  `startup_required` and `db_locked` are retryable — follow the hint and retry.
- **Health endpoint.** Under HTTP transport, `GET /health` returns
  `{status, db_ok, uptime_s, version, pid, profile}` and answers independently of
  MCP session state, which makes it usable as a liveness probe for a supervisor.

## Recall

`query_memory` fuses three channels with reciprocal rank fusion:

- **Full-text** — SQLite FTS5 over event text.
- **Vector** — local embeddings (`nomic-embed-text` via Ollama by default).
- **Recency** — a temporal prior over recent events.

The vector channel is optional. With no embedding backend reachable, recall stays
online on full-text plus recency and reports `degraded: true` with the channels it
actually used, instead of failing the query.

## Configuration

Per-installation identity lives in the manifest, not in code:

```toml
[settings]
timezone = "Europe/Warsaw"
mem0_collection = "chronicle_personal"   # Mem0 collection the outbox targets
operator = "Ada Lovelace"                # shown in agent-facing prompts
```

Automation (optional, macOS launchd) takes its namespace from
`CHRONICLE_AUTOMATION.toml`:

```toml
[paths]
launchd_label_prefix = "com.example"
```

## Architecture

```text
agents / CLI / MCP clients
          |
          v
   Max Chronicle service
          |
          +--> chronicle.db
          |    SQLite truth store, schema v9
          |    events | snapshots | entities
          |    situation models | lenses | scenarios
          |
          +--> Markdown projections
          |    readable derived state
          |
          +--> Mem0 (optional, external bridge)
               derived semantic recall
```

Queries combine local Chronicle data, readable source projections, and optional semantic recall. The database remains authoritative; projections and recall layers do not write truth back into Chronicle.

## License

MIT
