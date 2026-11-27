# Install Max Chronicle

This repository now supports a safe two-layer install model:

- `core`: Chronicle CLI, SQLite truth store, MCP server, SSOT docs, snapshots, projections
- `optional`: launchd automation, external runtime feeds, repo hooks

The goal is to let someone install Chronicle without inheriting your personal machine layout.

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

Create a fresh workspace:

```bash
chronicle init --root ~/chronicle-workspace
export CHRONICLE_ROOT=~/chronicle-workspace
```

Smoke-test the install:

```bash
chronicle status
chronicle startup --domain global --format json
chronicle activate --domain global
chronicle-mcp --profile chronicler
```

## Start the HTTP server and connect an agent

Agents reach Chronicle through its HTTP server. Start it with the workspace set:

```bash
export CHRONICLE_ROOT=~/chronicle-workspace
MCP_PORT=8000 chronicle-mcp --transport streamable-http --migrate
```

The server writes its address to `chronicle-server.json` in the workspace and
removes it on a clean stop. There is no default port for the commands below:
they take `--url`, else `CHRONICLE_SERVER_URL`, else that file.

```bash
chronicle connect claude --dry-run      # what would change, nothing written
chronicle connect claude                # shows the change and asks; --yes applies without asking
chronicle connect codex --role read     # roles: write (default) and read
chronicle connect claude --skip hook    # leave out a part: permissions, hook or block (repeatable)
chronicle connect gemini --print        # snippets and the block for adding by hand
chronicle agents
chronicle doctor --agents
chronicle disconnect claude
```

Agents with an adapter: Claude Code (`claude`), Codex (`codex`), ZCode (`zcode`),
opencode (`opencode`), Gemini CLI (`gemini`). Cursor and other agents get the
snippets with `chronicle connect <agent> --print`. Before any change, `connect`
checks that the server answers, is Chronicle 0.33.0 or newer and serves
`/mcp/read`; if not, it changes nothing and prints how to start the server.

| Role | Address | Tools |
| --- | --- | --- |
| `write` (default) | `/mcp` | all six |
| `read` | `/mcp/read` | five, without `record_event` |

A role is a guard-rail against mistakes, not a security boundary: an agent with a
shell on the same machine can read the database and other agents' files directly.

The session-start hook is part of the package: `connect` writes
`<absolute python> -m max_chronicle.brief_hook --manifest <workspace manifest>` into
the agent's hook settings. It prints the brief from the server, or from the database
with one line saying the server is silent, and always exits 0. Codex runs a new hook
only after it is trusted in `/hooks`; opencode has no session-start hook, so its
block tells the agent to start with `startup_bundle`. If the agent already runs a
session-start hook of its own that mentions chronicle, `connect` prints a notice
naming the file; `--skip hook` keeps only that hook, so a session gets one brief.
A skipped part is reported as `skipped (asked)`, `doctor --agents` takes it as
intended, and a later `connect` without `--skip` installs it.

`chronicle connect` runs on macOS and Linux; on Windows it refuses with a message.

## What `chronicle init` Creates

- `chronicle.db`
- `SSOT_MANIFEST.toml`
- `CHRONICLE_AUTOMATION.toml`
- starter SSOT docs
- placeholder runtime files for optional integrations

The generated workspace is intentionally additive and local. It does not modify your current repository state unless you point `--root` at an existing directory and pass `--force`.

## Optional Integrations

These are not required for the core system to work:

- local vector recall through Ollama: `ollama pull qwen3-embedding:0.6b`, then `chronicle embed-backfill`;
  `[embedder] start_command` lets Chronicle start Ollama when it is down ([OPERATIONS](docs/OPERATIONS.md))
- launchd scheduling
- external digest feeds
- repo hook automation

Fresh installs can leave those placeholders as-is until the core capture/query flow works.

## Package Scripts

Installed entrypoints:

- `chronicle`
- `chronicle-mcp`
- `chronicle-mcp-readonly`
- `chronicle-mcp-chronicler`

If you run from this repository directly, the wrapper scripts in `scripts/` now resolve the project root dynamically instead of depending on one absolute path.

## Building a Shareable Distribution

Editable install is enough for most teammates. If you want a wheel:

```bash
python -m build
```

Then share the generated artifact from `dist/`.

## Recommended Rollout

1. Install the package.
2. Run `chronicle init --root ...`.
3. Confirm `chronicle status` and `chronicle startup` work.
4. Customize `SSOT_MANIFEST.toml`.
5. Customize `CHRONICLE_AUTOMATION.toml`.
6. Only then enable optional integrations.

## Agent handoff

Start with `chronicle startup --project demo --task-id ship --format json`.
See README for checkpoints, explicit facts, request IDs, and MCP configuration.
Build distributions from the public source checkout, with the private workspace
kept in a separate directory.
