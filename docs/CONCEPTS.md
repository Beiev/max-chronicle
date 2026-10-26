# Concepts

Chronicle keeps a few kinds of records. Every one names its author and when it
was recorded; none is rewritten when the world changes, a newer record replaces
it and the old one stays inspectable.

```text
            agent / CLI / MCP
                   |
             record_event ------------------------------.
          /        |          \                          |
     event    observation    fact (slot = value)    checkpoint
  (what, why,  (who said it,  current until a       (a task's handoff:
   evidence)    in which      newer fact names it    goal, completed,
                session)      in `supersedes`)       verification, open
                                                     questions, next steps)
     notes: files agents keep on purpose, indexed for recall (read-only)
```

- **Event.** Something that happened or was decided: text, `why`, category,
  evidence files (archived by content hash, secrets redacted). The same text
  recorded twice is one event with two observations.
- **Observation.** Who recorded an event, when, in which project, task, domain and
  session. Confirmations by other agents keep every author.
- **Fact.** A current assertion under a stable `slot` (`deployment.target`,
  `policy.capture`) with a `kind`: `observed`, `decision` or `assumption`. A fact
  stays current until a newer one names it in `supersedes`; prose is never
  promoted into a fact on its own.
- **Checkpoint.** A task's handoff, attached to an event: goal, completed work,
  verification actually run, open questions, next steps. The latest checkpoint is
  what the next agent resumes from.
- **Task.** A `project` plus a `task_id`, shared by every agent on the work. A task
  is open until its `task.status` fact says `completed` or `cancelled`.
- **Note.** A file an agent keeps on purpose (such as Claude's file memory),
  indexed section by section so every agent can recall it. Chronicle never writes
  to a note.
- **Evidence.** Files attached to an event, archived as content-addressed copies,
  or kept as a pointer when archiving is off. A pointer is not a copy.
- **Scope.** `domain` is a broad configured area; `project` and `task_id` identify
  the work. Recall, the brief and task context filter by scope.
- **Quarantine.** An event marked `memory_guard.visibility = raw_only` stays in the
  history but never reaches recall, briefs, task context, `get_memory` or the web view.

A stored record is an attributed assertion, not a verified fact: its author,
evidence and kind stay visible so the reader can judge it.

## A handoff

MCP `record_event` arguments:

```json
{
  "text": "Offline recall implemented; vector evaluation remains",
  "why": "The next agent can continue from the verified lexical path",
  "project": "demo",
  "task_id": "ship",
  "request_id": "demo-ship-checkpoint-1",
  "checkpoint": {
    "goal": "Ship reliable recall",
    "completed": ["Implemented lexical search"],
    "verification": ["Offline search test passed"],
    "open_questions": ["Which vector threshold works for this corpus?"],
    "next_steps": ["Evaluate vector precision on representative queries"]
  }
}
```

MCP assigns a session identity automatically. Independent confirmations preserve
both authors and attachments even when the underlying event text is deduplicated.
`chronicle_status` acknowledges the event; `observation_id` identifies this
observation. `evidence` reports each attachment as archived, pointer-only, missing,
or skipped. A pointer is not an archived copy.


## Current knowledge and corrections

Supply an explicit `fact` object on `record_event`:

```json
{"slot": "project.status", "value": "active", "kind": "observed"}
```

The receipt includes `fact_id`. To change that scoped slot, include its current
ID in the next assertion:

```json
{"slot": "project.status", "value": "paused", "kind": "decision", "supersedes": "<current-fact-id>"}
```

Chronicle retains the old assertion and its evidence. Stale replacements fail
with an actionable error. Current recall excludes superseded assertions; historical
queries retain the event trail. Unstructured prose is never automatically promoted
into a fact, and assumptions are not silently converted into verified knowledge.


## One name per project, domain and agent

One project often gets written several ways (`status`, `chronicle`,
`max-chronicle`), and so do tasks (`Launch_plan`, `launch_plan`) and agents
(`Codex`, `codex-mcp-client`). Names match whatever their case, spaces and
underscores, and the manifest lists the other spellings of a project, domain
or agent:

```toml
[[projects]]
id = "atlas"
roots = ["~/code/atlas"]
aliases = ["atlas-app", "old-atlas"]

[[domains]]
id = "memory"
aliases = ["mem"]  # next to the domain's label and source_ids

[[agents]]
id = "claude"
aliases = ["assistant"]  # exact spellings
prefixes = ["claude"]    # claude-mac, claude-code, ...
```

A filter by any spelling finds what was written under every one of them
(FR-14): recall, task context, open tasks, current facts, the brief and
cursors. History is not rewritten: a write stores the canonical name and keeps
the given one as `actor_raw`, `project_raw` or `domain_raw`. The common agents
(claude, codex, glm, deepseek, gemini, opencode, transcript-analyst) are
built in; an `[[agents]]` entry with the same id replaces a built-in one. A
session that never names its agent is attributed to its MCP client, such as
`claude-code`.

