# Working on Chronicle

Chronicle is shared memory for agents. Preserve attributed evidence and make
another agent's continuation reliable. Keep the public package independent of
any operator's personal workspace.

## Read first

- `README.md`: client workflow, tool contract, and examples.
- `max_chronicle/docs/SCHEMA_V1.md`: current schema (filename retained for old links).
- `max_chronicle/docs/ROADMAP-ULTIMATE-MEMORY.md`: accepted requirements and checks.

## Code map

- `service.py`: record/startup coordination and compatibility entrypoints.
- `memory.py`: explicit observations, checkpoints, request IDs, fact revisions.
- `recall.py` / `embeddings.py`: eligible candidates, ranking, local vector backend.
- `store.py` / `db.py`: transactions, archive primitives, migrations.
- `redaction.py`: content-based secret filter applied before anything is stored.
- `mcp_server.py` / `cli.py`: thin, documented client adapters.
- `native_automation.py`: optional scheduling, backups, restore checks.
- `scaffold.py`: neutral new-workspace templates.

## Invariants

- A stored assertion has provenance; storage does not prove its factual accuracy.
- Old events and superseded decisions stay inspectable.
- Retrying a request cannot duplicate or change its committed observation.
- Scope/visibility filters apply before ranking; unrelated recency is not evidence.
- External recall failure cannot prevent canonical local writes.
- Likely secrets are redacted before storage, archiving, or any derived output.
- Checkpoints record actual verification, uncertainty, and concrete next steps.
- A committed write and incomplete derived output have separate receipts.
- Backups must verify content, not only counts, and work without the source tree.

## Validation and release

Run `python -m pytest -q` from the checkout. Use temporary workspaces and synthetic
evidence; do not point tests at a real Chronicle database. Add behavioral tests
for new contracts, including two-agent handoff, retries, and failure paths.

Schema changes require a new numbered migration; do not edit applied migrations.
Keep existing CLI/MCP entrypoints compatible and update README/examples alongside
the implementation. Keep changes focused; avoid broad style-only rewrites.

Build from this clean source checkout. Inspect both wheel and source archive for
personal paths, credentials, databases, logs, and generated workspace files.
Never export a whole private workspace into this repository.
