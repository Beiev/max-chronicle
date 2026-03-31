# Max Chronicle

Local-first durable memory for AI agents.

Max Chronicle is a Python package for multi-agent systems that need context to survive across sessions. It keeps canonical history in local SQLite, exposes that history through a CLI and MCP server, and can attach an optional Mem0 semantic recall layer. It is not a cloud service; the core system runs entirely on your machine.

Core capabilities include append-only events, point-in-time snapshots, hybrid local search, normalized entities, situation models, fixed analytical lenses, and what-if scenario analysis.

## Why

Agents are good at short-lived reasoning and bad at durable memory. Between sessions, threads, and tool calls, they lose state, repeat work, and blur truth with generated summaries.

Max Chronicle fixes that by separating local truth from derived views: record durable events and snapshots into SQLite, project readable state into Markdown, and let the next agent resume from an explicit startup or activation bundle instead of starting from zero.

## Quick Start

Requirements:

- Python 3.11+
- `pip` or `uv`

Install from the repository:

```bash
git clone <your-repo-url> max-chronicle
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
| `chronicle.db` | Canonical truth. Local SQLite, schema v7. Stores append-only events, point-in-time snapshots, normalized entities, situation models, analytical lens runs, and scenario runs. |
| Markdown projections | Readable operator-facing views derived from Chronicle. Useful for inspection, handoff, and versioned docs, but not the source of truth. |
| Mem0 / Qdrant / Kuzu | Optional semantic recall layer. Useful for retrieval and association, but derived and non-authoritative. |

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

Derived analysis commands are also available:
`normalize-entities`, `build-situation`, `run-lenses`, `record-scenario`, and `review-scenario`.

## MCP Server

Max Chronicle ships an MCP stdio server for agent integration.

- `chronicle-mcp-readonly` exposes read-only resources and tools.
- `chronicle-mcp-chronicler` exposes the full chronicler surface, including writes.

The generic entrypoint `chronicle-mcp --profile readonly|chronicler` is also available.

## Architecture

```text
agents / CLI / MCP clients
          |
          v
   Max Chronicle service
          |
          +--> chronicle.db
          |    SQLite truth store, schema v7
          |    events | snapshots | entities
          |    situation models | lenses | scenarios
          |
          +--> Markdown projections
          |    readable derived state
          |
          +--> Mem0 / Qdrant / Kuzu
               optional derived semantic recall
```

Queries combine local Chronicle data, readable source projections, and optional semantic recall. The database remains authoritative; projections and recall layers do not write truth back into Chronicle.

## License

MIT
