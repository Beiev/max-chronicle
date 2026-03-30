# Memory V2 Plan
> Status: proposed
> Date: 2026-03-21
> Scope: Chronicle-centered memory evolution
> Baseline: Chronicle is already the canonical truth layer. V2 adds a situation model and analytical layer without weakening trust.

## Goal

Turn memory from a clean archive of facts into an inspectable, evidence-linked model of the current situation.

Memory V2 should support:
- durable factual capture
- explicit interpretation layers
- scenario and counterfactual analysis
- explainable briefing and interrogation
- objective source weighting by freshness, trust, and provenance

It must not turn speculation into durable truth.

## Core Thesis

The strongest idea to borrow from MiroFish is not swarm scale or "prediction".

It is this pattern:

`captured facts -> structured situation model -> fixed lenses -> scenario analysis -> report -> interrogation`

For Chronicle, that becomes:

`Chronicle truth -> structured situation synthesis -> lens runs -> scenario runs -> briefing layer -> question answering`

## What We Borrow

Validated against the public MiroFish, OASIS, and Graphiti materials:

- visible, stateful pipeline instead of black-box summarization
- explicit project/task progress and post-run review
- graph-like situation structure over unstructured material
- report plus interrogation over an already-built model
- temporal validity, provenance, and contradiction handling
- hybrid retrieval instead of semantic recall alone

## What We Do Not Borrow

- "predict anything" positioning
- large social-media simulation as a default architecture
- swarm scale as a value by itself
- storing inferred reasoning as authoritative memory
- provider- or cloud-coupled architecture as a system requirement

## Non-Negotiable Trust Rules

Memory V2 has four classes of knowledge:

1. `truth`
   What happened or is directly observed.

2. `interpretation`
   Structured meaning derived from truth.

3. `scenario`
   Conditional analysis under explicit assumptions.

4. `briefing`
   User-facing synthesis assembled from the above.

Hard rule:
- `truth` may become `interpretation`
- `interpretation` may become `scenario`
- none of `interpretation`, `scenario`, or `briefing` may be written back as `truth`

## Canonical Source Hierarchy

This extends the existing Chronicle trust model:

1. Chronicle DB events, artifacts, and snapshots
2. first-party runtime evidence
3. curated SSOT and projections
4. external source-ranked world feeds
5. Mem0 semantic recall

Mem0 remains a derived recall/index layer.

## Source Classes For V2

### A. Canonical Truth

- Chronicle events
- Chronicle artifacts
- Chronicle snapshots
- git hook events
- durable decisions, milestones, implementations

### B. First-Party Runtime

- OpenClaw runtime state
- leads and morning brief
- watchdog and health data
- email triage summaries
- repo state and asset manifests
- daybooks

### C. External World

- intel-digest synthesized world stream
- structured job-market feeds
- company intelligence
- selected first-party company pages and filings when needed

### D. Derived Layers

- situation models
- lens reports
- scenario runs
- briefings
- Mem0 recall packages

## Main Gaps In The Current System

- trust and freshness are documented, but not yet universal ranking signals
- provenance is not consistently carried through to every derived layer
- company and entity normalization is weak
- there is no first-class `situation model` object
- there is no replay/evaluation layer for forecasts or counterfactuals
- external-world and life/personal signals are unevenly covered
- some projections look authoritative even when they are already interpretation

## Memory V2 Architecture

### Layer 0. Chronicle Truth

Keep Chronicle as the only canonical writer for durable truth.

Required properties:
- append-only durable history
- exact timestamps
- artifact archiving
- source references
- strict write contracts

### Layer 1. Source Governance

Add one machine-readable source catalog.

Each source should declare:
- `source_id`
- `domain`
- `class`
- `owner`
- `freshness_policy`
- `trust_tier`
- `questions_it_can_answer`
- `questions_it_cannot_answer`
- `normalization_rules`

This becomes the basis for audit, retrieval ranking, and report provenance.

### Layer 2. Structured Situation Model

Build a compact state model from truth, not from Mem0.

A situation model should capture:
- actors
- entities
- active goals
- pressures
- constraints
- contradictions
- open questions
- risks
- recent transitions
- expected next decision points

The situation model is derived and versioned.
It should point back to:
- snapshot id
- source event ids
- artifact ids
- freshness/trust summary

### Layer 3. Fixed Lenses

Instead of arbitrary swarms, run a small set of stable analytical lenses.

Initial lenses:
- `operator`
- `strategist`
- `risk`
- `market`
- `systems`

Each lens may read truth and situation models, but it only writes derived outputs.

### Layer 4. Scenario Engine

Add counterfactual and pressure-test runs.

Each scenario run must store:
- `scenario_id`
- `based_on_situation_id`
- `assumptions`
- `changed_variables`
- `expected outcomes`
- `failure modes`
- `confidence`
- `review_due_at`

A scenario is never truth.

### Layer 5. Briefing And Interrogation

Generate compact evidence-linked briefings.

A briefing should answer:
- what is true now
- what changed recently
- what matters most
- what is uncertain
- what likely follows under scenario A/B

Interrogation should query the current situation model and cite evidence, not freestyle from raw recall.

## Suggested Data Contracts

Memory V2 does not require a rewrite, but it does need new structured objects.

Recommended new object families:
- `source_catalog`
- `normalized_entities`
- `situation_models`
- `situation_assertions`
- `lens_runs`
- `scenario_runs`
- `briefing_runs`
- `forecast_reviews`

Each derived object should carry:
- `kind`
- `derived_from`
- `confidence`
- `valid_at`
- `superseded_at`
- `provenance`

## Retrieval Rules In V2

Retrieval should rank by:
- domain relevance
- source class
- freshness
- trust tier
- exactness
- contradiction state

Derived summaries must never outrank fresher canonical evidence when they conflict.

Queries should be able to request:
- `truth only`
- `truth + interpretation`
- `truth + interpretation + scenarios`

## Ingestion Rules

### Truth Ingestion

Allowed:
- observed events
- first-party runtime measurements
- directly sourced external facts with provenance
- explicit decisions with reasons

Not allowed:
- vibes
- speculative forecasts
- ungrounded summaries
- model-written claims without evidence

### Interpretation Ingestion

Allowed:
- entity consolidation
- pressure mapping
- contradiction detection
- state summaries
- vector and trajectory analysis

Must always link back to truth.

### Scenario Ingestion

Allowed:
- what-if branches
- A/B decision consequences
- likely failure modes
- stress tests

Must always include assumptions and confidence.

## Coverage Expansion For A Real Chronicle

To approach the "knows what is happening in work, life, agents, world, priorities, and prospects" goal, V2 should explicitly add or improve:

- company intelligence as a first-class refreshed source
- stronger entity normalization for people, companies, projects, and topics
- life/admin lane for calendar, money, obligations, and health if enabled
- world lane split into geopolitics, economy, AI, and job market
- agent-performance lane for runs, failures, costs, throughput, and drift
- decision-review lane that checks whether earlier expectations matched outcomes

## Delivery Plan

## Phase 1 — Source Audit And Governance

Ship:
- `chronicle sources audit`
- source catalog with trust/freshness metadata
- explicit provenance in retrieval results
- staleness and coverage report across all active sources

Exit criteria:
- every answerable domain shows its source set and freshness state
- stale or weak sources are visible by default

## Phase 2 — Entity And Fact Hygiene

Ship:
- entity normalizer for companies, people, projects, and topics
- stronger dedupe between Chronicle, projections, and Mem0
- explicit fact schema with evidence links

Exit criteria:
- major entity collisions are reduced
- duplicate meaning is flagged instead of silently multiplied

## Phase 3 — Situation Synthesis

Ship:
- `situation_model` builder from Chronicle snapshots and events
- structured outputs for actors, pressures, constraints, contradictions, open questions
- domain-specific situation views for `global`, `career`, `portfolio`, `agents`, `world`

Exit criteria:
- the system can build a current-state model from evidence without using freeform summary alone

## Phase 4 — Lens Layer

Ship:
- fixed lens runs
- lens registry and prompts/contracts
- evidence-linked lens outputs

Exit criteria:
- the same situation can be reviewed from multiple stable perspectives without drift

## Phase 5 — Scenario Layer

Ship:
- counterfactual runner with assumptions
- scenario objects and storage
- "what likely happens if A/B" interrogation path

Exit criteria:
- scenario analysis is queryable and clearly separated from truth

## Phase 6 — Replay And Evaluation

Ship:
- forecast review objects
- compare predicted vs actual outcomes
- confidence calibration checks

Exit criteria:
- the system learns which analytical patterns are reliable

## Phase 7 — Briefing Interface

Ship:
- compact daily and on-demand briefings
- interrogation over situation models
- richer operator and agent attach bundles

Exit criteria:
- agents and humans can interrogate the current state model instead of scraping raw memory

## Immediate Next 3 Tasks

1. Build `chronicle sources audit` and a machine-readable source catalog.
2. Add entity normalization and first-class company-intel ingestion.
3. Define the `situation_model` schema and generate the first read-only version from existing Chronicle data.

## Success Criteria

Memory V2 is successful when:
- truth remains clean and authoritative
- interpretation becomes structured and inspectable
- scenarios are useful but clearly conditional
- reports cite evidence instead of masking it
- source freshness and trust become visible system properties
- new analytical tools can attach without repo archaeology

## Final Rule

The memory system should become more powerful by becoming more explicit, not more magical.
