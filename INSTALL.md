# Install Max Chronicle

This repository now supports a safe two-layer install model:

- `core`: Chronicle CLI, SQLite truth store, MCP server, SSOT docs, snapshots, projections
- `optional`: Mem0 bridge, launchd automation, external runtime feeds, repo hooks, model-backed canaries

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

## What `chronicle init` Creates

- `chronicle.db`
- `SSOT_MANIFEST.toml`
- `CHRONICLE_AUTOMATION.toml`
- starter SSOT docs
- placeholder runtime files for optional integrations
- a stub `scripts/mem0_bridge.py` so fresh installs stay safe

The generated workspace is intentionally additive and local. It does not modify your current repository state unless you point `--root` at an existing directory and pass `--force`.

## Optional Integrations

These are not required for the core system to work:

- Mem0 / vector recall
- launchd scheduling
- external digest feeds
- repo hook automation
- MiniMax canaries

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
