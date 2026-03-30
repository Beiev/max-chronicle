# Max Chronicle

Local-first durable memory system for AI agents — SQLite truth store, MCP server, semantic recall, time-travel reconstruction.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests: 88 passing](https://img.shields.io/badge/tests-88%20passing-brightgreen.svg)]()
[![Schema: v7](https://img.shields.io/badge/schema-v7-blue.svg)]()

## Features

- **Append-only event store** in SQLite — canonical truth, not LLM-generated summaries
- **Point-in-time snapshots** with full state reconstruction
- **MCP server** with two profiles: read-only and full chronicler
- **Semantic recall** via Qdrant vector search + Kuzu graph DB + BM25/RRF hybrid retrieval
- **Automated projections** — normalized entities, situation models, evidence-linked lenses
- **Five analytical lenses** — operator, strategist, risk, market, systems
- **What-if scenarios** — record assumptions, replay against outcomes
- **Native macOS launchd automation** — daily capture, backup, audit, canary
- **Mem0 integration** for semantic replication from truth store
- **Guided Startup Gate** — MCP sessions require explicit activation before mutations

## Architecture

```
┌─────────────────────────────────────────────────┐
│               MCP Clients (Agents)              │
├─────────────────────────────────────────────────┤
│             MCP Server Layer                     │
│   ┌─────────────┐  ┌─────────────────────────┐  │
│   │  Read-Only   │  │    Full Chronicler      │  │
│   └─────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────┤
│              Service Layer                       │
│  activate · startup · query · record · capture   │
├─────────────────────────────────────────────────┤
│            Derived Layers                        │
│  Entities │ Situations │ Lenses │ Scenarios      │
├─────────────────────────────────────────────────┤
│         Query Engine (Hybrid Retrieval)          │
│  Qdrant (vectors) + Kuzu (graph) + BM25 (text)  │
├─────────────────────────────────────────────────┤
│              SQLite (Append-Only Truth)          │
│  Events │ Snapshots │ Artifacts │ Relations      │
└─────────────────────────────────────────────────┘
```

### Truth Model

Chronicle follows a strict layered truth model:

1. `chronicle.db` — canonical durable truth (events, snapshots, artifacts)
2. `status/*.md` — human-readable projections (generated, not hand-edited)
3. Mem0 / vector recall — derived semantic memory (never the source of truth)

No derived layer writes back into truth.

## Quick Start

```bash
# Clone and install
git clone https://github.com/rzmrn/max-chronicle.git
cd max-chronicle
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# Initialize a workspace
chronicle init --root ~/chronicle-workspace
export CHRONICLE_ROOT=~/chronicle-workspace

# Verify
chronicle status
chronicle startup --domain global --format json
```

## Core Commands

```bash
# Record a durable event
chronicle record "Decided to use PostgreSQL for the new service" --why "Better JSON support than SQLite for this use case" --category decision --project my-project

# Capture runtime state
chronicle capture-runtime --domain global

# Query chronicle
chronicle query "current blockers" --domain global --format json

# Time-travel reconstruction
chronicle timeline --at "2026-03-15T12:55:00+01:00"

# View recent events
chronicle recent --limit 10

# Run audit
chronicle doctor --strict
```

## MCP Server

Two access profiles for different agent roles:

```bash
# Read-only — for agents that query but never mutate
chronicle-mcp-readonly

# Full chronicler — record events, capture snapshots, manage derived state
chronicle-mcp-chronicler
```

The chronicler profile requires a startup call (`startup_bundle` or `activate_agent`) before any mutations. This prevents accidental writes from misconfigured agents.

## Optional Integrations

These are not required for the core system:

- **Mem0 / Qdrant** — semantic vector recall
- **Kuzu** — graph-based entity relations
- **launchd** — scheduled automation (daily capture, backup, audit)
- **MiniMax / OpenAI** — LLM-powered curation and canary checks

The core `record → query → reconstruct` flow works with SQLite alone.

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## License

MIT License. See [LICENSE](LICENSE) for details.
