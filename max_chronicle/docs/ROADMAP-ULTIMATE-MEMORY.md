# Ultimate Memory Roadmap
> Status: archived on 2026-03-29
> Created: 2026-03-16
> Baseline: Chronicle is already stable and working. This roadmap is historical context only.

Chronicle was declared finished on 2026-03-29.

No further roadmap phases are active.

Future work is maintenance-only unless bugs are found.

The phase breakdown below is preserved for design history, not as an active execution plan.

## Goal

Build a local-first memory system that stays trustworthy, current, inspectable, and universally attachable for any future agent.

Success means:
- any agent can attach through one stable contract
- canonical truth stays in Chronicle without ambiguity
- context retrieval prefers fresh and trustworthy evidence
- stale, duplicate, or low-signal memory is surfaced and controlled
- time-travel reconstruction stays reliable years later

## Decision Rules

Prioritize only work that improves at least one of these:
- universal agent interoperability
- data freshness
- data cleanliness and dedupe
- source trust and evidence quality
- retrieval quality and sorting
- long-term durability and restore confidence

Do not prioritize work that adds new surfaces without strengthening those properties.

## Confirmed Bottlenecks

### 1. Split universal contract

Today `max-chronicle activate` still routes through `ssot_hub.py`, while the canonical service layer, MCP server, projections, and automation live in `max_chronicle`.

Why this matters:
- two attachment paths increase drift risk
- future agents can receive slightly different behavior depending on entrypoint
- improvements to ranking, freshness, and validation must be implemented twice or will diverge

### 2. Freshness is observed, but not yet enforced

Chronicle captures source mtimes and runtime state, but freshness is not yet a first-class guardrail in retrieval and projections.

Why this matters:
- an attached agent can read old `mem0-dump.json` or stale generated views without strong warning
- current ranking does not penalize stale evidence enough
- the system knows "when data was seen" better than it currently uses

### 3. Trustworthiness is implicit, not modeled

The manifest defines sources and priorities, but there is no explicit trust/freshness score used everywhere in query, activation, projection, and future retrieval.

Why this matters:
- reliable sources and derivative summaries are not consistently separated
- future expansion to more connectors will create noise unless trust is formalized
- sorting quality depends too much on simple textual match

### 4. Event/entity governance is still loose

Categories, entity types, and relation semantics are only partially validated. Duplicate meaning can still enter via multiple write paths.

Why this matters:
- low-governance timelines get noisier every month
- semantic sync quality depends on stable categories and entities
- universal reuse requires predictable data contracts

### 5. Snapshot capture still has coupled side effects

Runtime capture also triggers projections. That is convenient, but it ties archival truth to operator-facing file generation.

Why this matters:
- universal agents should be able to capture state without accidentally mutating docs unless requested
- projection failures should not weaken canonical capture
- freshness workflows become harder to reason about

### 6. Long-term durability needs more proof, not more features

Backups and audits exist, but the system still needs stronger restore drills, corruption checks, and invariant monitoring.

Why this matters:
- "works now" is not enough for a multi-year memory system
- long-lived systems fail at restore time, not at insert time

## Prioritized Plan

## Phase A — Unify The Agent Contract

Priority: highest

Ship:
- move activation, query, and timeline fully behind `max_chronicle` service primitives
- keep `ssot_hub.py` as a thin compatibility shell, not a second logic layer
- version the activation contract
- add one machine-readable "attach bundle" for agents: current snapshot, recent events, source freshness, trust hints, and operating rules

Why this is first:
- it removes the main drift vector
- every future improvement becomes universal automatically

Exit criteria:
- `max-chronicle`, `chronicle`, and `chronicle-mcp` read from the same service path for activation/query/timeline
- legacy JSONL fallback is compatibility-only and clearly isolated

## Phase B — Make Freshness A First-Class Retrieval Signal

Priority: highest

Ship:
- define freshness classes for sources: live, recent, stale, archival
- add freshness weighting in `query_context`, activation assembly, and future ranking
- emit explicit stale-source warnings in MCP resources and activation prompts
- add audit checks for stale projections, stale mem0 dump, and stale external sources

Why this is real leverage:
- improves answer quality without adding more data
- reduces silent use of old context

Exit criteria:
- every retrieval response can explain how fresh its winning evidence is
- stale data is visible and penalized by default

## Phase C — Add Source Trust And Evidence Ranking

Priority: high

Ship:
- define trust tiers in the manifest and persist them into retrieval results
- distinguish canonical, primary runtime, derived projection, and semantic recall sources
- rank retrieval by exactness + freshness + trust + domain relevance
- prefer raw evidence paths over summaries when they conflict

Why this matters:
- "good sorting" depends on more than keyword hits
- it prevents derived layers from outranking truth

Exit criteria:
- query results show source class and ranking basis
- lower-trust summaries no longer outrank fresher canonical evidence

## Phase D — Tighten Data Hygiene And Governance

Priority: high

Ship:
- formal category registry for durable events
- stronger entity and relation validation
- dedupe rules for Chronicle writes and Chronicle -> Mem0 sync
- explicit rules for what is allowed into snapshots vs events vs projections vs Mem0
- reconciliation jobs that flag duplicate meaning across Chronicle, projections, and Mem0

Why this matters:
- this is what keeps the system clean after six months, not after one demo

Exit criteria:
- new writes are predictable and easier to lint
- duplicate or malformed durable memory gets caught automatically

## Phase E — Decouple Capture From Projection

Priority: medium-high

Ship:
- separate "capture truth" from "render operator views"
- keep a convenience combined command, but make the primitives distinct
- ensure projection failure never marks canonical capture as failed
- store projection freshness and lineage more explicitly

Why this matters:
- reduces accidental side effects for future agents
- improves operational reasoning and rollback safety

Exit criteria:
- snapshots can be captured without mutating markdown
- projection generation is an explicit secondary step

## Phase F — Harden Multi-Agent Interoperability

Priority: medium-high

Ship:
- add stable MCP resource names for current attach state, domain attach state, and health
- return more structured retrieval payloads for agents, not only human-readable text
- include contract version, source freshness, and trust metadata in agent-facing outputs
- create a minimal external-agent integration guide with examples for MCP and CLI attachment

Why this matters:
- universal systems fail when the contract is implicit
- future agents should not need repo archaeology

Exit criteria:
- a new agent can attach using only the MCP contract and docs without custom glue

## Phase G — Prove Durability Operationally

Priority: medium

Ship:
- scheduled restore drills with recorded evidence
- checksum verification for DB and artifact store
- invariant audit: orphaned artifacts, broken links, invalid JSON payloads, projection lag, stale source lag
- clearer "system health" status for Chronicle itself

Why this matters:
- this is the difference between a useful system and a dependable one

Exit criteria:
- restore confidence is measured, not assumed
- durability regressions create visible events

## Not A Priority Right Now

These are feature-shaped, but not current bottlenecks:
- moving from SQLite to Postgres
- adding a heavy graph UI
- adding more LLM summarization layers
- broad auto-tagging without stronger governance first
- cross-agent write concurrency until single-writer discipline is actually insufficient
- replacing Mem0 just because it is imperfect

## Recommended Delivery Order

1. Phase A — Unify the agent contract
2. Phase B — Make freshness first-class
3. Phase C — Add source trust and evidence ranking
4. Phase D — Tighten data hygiene and governance
5. Phase E — Decouple capture from projection
6. Phase F — Harden multi-agent interoperability
7. Phase G — Prove durability operationally

## Immediate Next Moves

If work starts now, the first concrete implementation slice should be:
- refactor activation/query/timeline so `ssot_hub.py` delegates into `max_chronicle`
- add explicit `source_class`, `trust_tier`, and `freshness_status` to retrieval outputs
- add one Chronicle audit for stale sources and stale projections

This gives the highest leverage with the lowest risk of feature drift.
