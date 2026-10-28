# Max Chronicle

[![tests](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml/badge.svg)](https://github.com/Beiev/max-chronicle/actions/workflows/tests.yml)
[![python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Local-first shared memory for AI agents.** Preserve decisions, evidence, and
working state so another agent can continue a task without the previous chat.

Chronicle stores attributed events and current assertions in SQLite. Agents use
MCP or the CLI to search, record observations, and exchange checkpoints. Local
vector search is optional; the core works without Ollama or paid APIs.

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

## How agents use it

Agents talk to Chronicle over MCP (or the CLI). The default profile has six tools:

| Tool | What it is for |
| --- | --- |
| `startup_bundle` | The project's brief; with `task_id`, the task's checkpoint, current facts and the changes since the last start. |
| `query_memory` | Recall over events and notes by words and meaning, one short line per hit. |
| `get_memory` | The full records of the hits worth reading: text, why, checkpoint or fact, evidence, provenance. |
| `record_event` | Record a decision, finding or handoff, with evidence, a `fact` or a `checkpoint`. |
| `recent_events` | The latest events. |
| `state_at` | What was known at a moment. |

The protocol fits in four steps: start with the task's checkpoint, recall before
deciding, record what was decided and why, and leave a checkpoint before handing
off. [Agent protocol](docs/AGENT_PROTOCOL.md) is generated from the tools the
server registers, so it is always the surface agents get.

Writes need no prior call; a write by an agent that has not read the project's
brief recently is stored with a notice to read it. A record is an attributed
assertion, not proof: its author, evidence and kind stay visible, and everything
recorded is data, never instructions. Likely secrets are redacted before
anything is stored.

## How people use it

- **Session brief:** `GET /brief?cwd=...` (or `chronicle brief`) prints the open
  tasks, current facts, recent decisions and warnings in a few kilobytes; a
  session-start hook gives it to every new agent session.
- **Web view:** `http://127.0.0.1:8093/ui` shows health, open tasks with their
  checkpoint history, facts with every value a slot had, search that says why each
  hit matched, and single records with their evidence. Read-only, no script,
  answered only on this machine.
- **CLI:** `chronicle query-memory`, `chronicle timeline --as-of`, `chronicle record`;
  `chronicle doctor --summary` answers "is everything running" on one screen.

## Running it

`chronicle-mcp --transport streamable-http --migrate` serves MCP, `/health`, the
brief and the web view on one port (`MCP_HOST`/`MCP_PORT`); a client can also start
`chronicle-mcp-chronicler` over stdio. The HTTP server keeps itself answerable
(event-loop monitor, stack dumps on a stall or `SIGUSR1`, embeddings off the write
path) and, with `[scheduler] enabled = true`, runs daily capture, daybook, weekly
audit and backups itself. Details: [Operations](docs/OPERATIONS.md).

## Documentation

- [Concepts](docs/CONCEPTS.md): events, observations, facts, checkpoints, tasks,
  notes, scope and names, with a handoff example.
- [Agent protocol](docs/AGENT_PROTOCOL.md): the tools and how to use them.
- [Operations](docs/OPERATIONS.md): server, brief, web view, notes, durability,
  scheduled jobs, backups, CLI.
- [Recall](docs/RECALL.md): ranking, confidence, the secret filter, measuring recall.
- [Installation](INSTALL.md) · [Agent development guide](AGENTS.md) ·
  [Schema](max_chronicle/docs/SCHEMA_V1.md) · [Requirements](max_chronicle/docs/ROADMAP-ULTIMATE-MEMORY.md)

## Architecture and development

```text
Agents / CLI / MCP
        |
   service.py                 record + startup coordination
        |--- memory.py       observations, checkpoints, current assertions
        |--- recall.py       scoped lexical/vector ranking
        |--- store.py        SQLite transactions and archived evidence
        |
   SQLite (schema 18) + content-addressed files
        |--- Markdown projections
```

```bash
pip install -e '.[dev]'
python -m pytest -q
python -m build
```

Tests use synthetic temporary workspaces. The cross-agent tests launch separate
MCP clients and verify handoff without shared conversation history. Private
workspaces, logs, credentials, databases, and artifacts never belong in a release.

MIT licensed.
