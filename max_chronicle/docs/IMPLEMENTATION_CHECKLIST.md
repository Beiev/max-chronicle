# Validation checklist

The active contract is [Shared agent memory](ROADMAP-ULTIMATE-MEMORY.md).

- [x] Scoped lexical/vector recall, no unrelated recency, index coverage reporting.
- [x] Independent observations, idempotent requests, explicit evidence receipts.
- [x] Two separate MCP clients can hand off and resume a task.
- [x] Current facts retain provenance and reject stale replacements.
- [x] Intact relocated backups pass; corrupt/missing evidence fails.
- [x] Existing tests and migration integrity checks pass.
- [x] Clean wheel/source archive and neutral fresh-workspace smoke test pass.
- [x] Operator installation upgraded and availability verified separately.

## v1.1

- [x] Explicit schema upgrades after a backup; stray servers cannot migrate (FR-9).
- [x] Golden-set eval with a pinned synthetic baseline (FR-13).
- [x] Unicode-aware lexical recall, flagged relaxed fallback, `no_confident_match` (FR-2).
- [ ] Secret filter on write, archive, and index (FR-11).
- [ ] Brief endpoint and session hooks for Claude and Codex (FR-12, NFR-4).
- [ ] Index of explicitly written notes with project/global scoping (FR-10).
- [ ] Identity registry and one scope function (FR-14).
