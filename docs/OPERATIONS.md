# Operations

Running Chronicle as a service: the MCP server, the session brief, the web view,
scheduled jobs, backups and schema changes. Recall itself is in [RECALL.md](RECALL.md),
the agent surface in [AGENT_PROTOCOL.md](AGENT_PROTOCOL.md).

## MCP server

Start `chronicle-mcp-chronicler` as a direct child process of the client. A typical
MCP configuration uses an absolute installed command and workspace path:

```json
{
  "mcpServers": {
    "chronicle": {
      "command": "/absolute/path/to/.venv/bin/chronicle-mcp-chronicler",
      "env": {"CHRONICLE_ROOT": "/absolute/path/to/chronicle-workspace"}
    }
  }
}
```

Use your client's equivalent configuration format. Stdio starts with the client
and does not require a separately running HTTP service. For supervised HTTP, use
`chronicle-mcp --transport streamable-http --migrate`; `MCP_HOST`/`MCP_PORT` set its address.
`GET /health` checks service and database availability and names the database,
manifest, schema version and installed code path that answered; `sources_audit` and
startup source health describe freshness. These are different checks.
One HTTP endpoint serves both MCP protocol eras (MCP Python SDK 2). A client
on `2026-07-28` sends each request on its own, with no session, so a server
restart does not interrupt it; an older client gets the `initialize` handshake
and a session, which a restart ends (the client starts a new one) and which
closes after 30 minutes without traffic.
Over HTTP, `/health` also reports the process: event-loop lag (`loop`), the write
queue (`writes`), the last committed write and the background embedder.
`GET /health/live` answers from the event loop alone, without the database: a slow
disk makes `/health` degraded (503), only a stuck loop silences `/health/live`.
When the loop stops for 10 s the server writes every thread's stack to its log by
itself, and `SIGUSR1` (advertised as `stack_dump_signal`) does the same on demand,
so a supervisor can record a hang before it restarts the process.

An HTTP server writes events without waiting for their vectors: a background
thread embeds them after the commit and backs off while Ollama is down. A sweep
with nothing to embed does not call Ollama, so the model stays loaded only for
`CHRONICLE_EMBED_KEEP_ALIVE` (default `5m`) after the last real call and Ollama
unloads it then. The model's context is capped at `CHRONICLE_EMBED_NUM_CTX`
(default 8192 tokens), which for `qwen3-embedding` halves its memory and leaves
shorter texts' vectors unchanged.

Chronicle can start Ollama when it finds it down, for example after a reboot:

```toml
[embedder]
start_command = ["open", "-g", "-a", "Ollama"]  # macOS app; or ["ollama", "serve"]
```

With it, a search waits up to 20 s for Ollama to come up and the model to load,
the load getting at least 3 s even when Ollama came up late, and answers on text
alone if it did not. A background sweep or note sync with work waits up to 60 s
for the start. A `startup_bundle` with `capture=true` holds the write slot, so
its recall waits 3 s at most and never starts Ollama. A start that did not bring
Ollama up is not repeated for a minute, and a start command that is still
running is not run again. Without the section Chronicle never starts anything,
and a search with Ollama down runs on text only, as before. `chronicle doctor`
shows whether Ollama is up, whether the model is loaded and until when;
`/health` adds the server's last start (`embedder.last_start`).


The default (`chronicler`) profile serves the six tools agents use
([AGENT_PROTOCOL.md](AGENT_PROTOCOL.md) has their full descriptions):

| Tool | Purpose |
| --- | --- |
| `startup_bundle` | The project's brief; with `task_id`, the task's checkpoint, current facts and change cursor. `mode="bundle"`: the full startup bundle. |
| `query_memory` | Scoped lexical/vector recall over events and notes, one short line per hit (`detail="full"` for whole records). |
| `get_memory` | Full records for `event:`/`note:` ids: text, why, checkpoint or fact, evidence, provenance. |
| `record_event` | Attributed observation, evidence, optional checkpoint or explicit fact. |
| `recent_events` | Recent event history. |
| `state_at` | Historical snapshots/events; `mode="as_of"` keeps only what was known then and adds the facts current then; `detail="full"` restores the full payload. |

`--profile admin` adds the maintenance surface, and `chronicle-mcp-readonly`
(`--profile readonly`) drops `record_event`:

| Admin tool | Purpose |
| --- | --- |
| `query_context` | Broader search through source documents. |
| `sources_audit` | Source coverage, freshness, and trust metadata. |
| `capture_snapshot` | Archive runtime state and update readable projections. |
| `entity_admin` | Report, normalize, alias, or merge entities. |
| `activate_agent` | Deprecated alias of `startup_bundle`. |

Failures set MCP `isError` and carry a JSON envelope with `error_type`,
`retryable`, and `hint`; follow that hint. A committed snapshot can separately
report `side_effect_errors` for failed evidence/projection outputs.


## Session brief

`GET /brief` returns a compact read-only brief (FR-12) that a new session reads
first:
- open tasks with their latest checkpoint;
- current facts;
- decisions from the last 14 days, each with its id;
- freshness and failed-job warnings;
- a three-line protocol.

It opens by saying it is data, not instructions. The brief reads through a
`query_only` connection and answers in milliseconds. It refuses any request that
carries an `Origin` header or a `Host` outside loopback, so a web page cannot
read it. Parameters:
- `cwd`: the working directory;
- `project`: a project slug that overrides `cwd`;
- `budget`: 1,000 to 8,000 characters, 6,000 by default;
- `format=json`: return JSON instead of plain text.

The project comes from `project`, from a configured root that holds `cwd`, or
from the name of `cwd` itself when that name is a known project. Parent
directories are not searched, so pass a repository's top level:

```toml
# SSOT_MANIFEST.toml
[[projects]]
id = "atlas"
roots = ["~/code/atlas", "~/code/atlas-worktrees"]
```

A session-start hook prints the brief and stays silent when the server is down:

```sh
dir=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
curl -fsS -m 2 --get --data-urlencode "cwd=$dir" http://127.0.0.1:8093/brief 2>/dev/null || true
```

Claude Code and Codex add a SessionStart hook's plain standard output to the
session context; Claude Code keeps up to 10,000 characters, above the brief's
maximum. Without HTTP, `chronicle brief --cwd DIR` prints the same text. An MCP client
calls `startup_bundle(project=...)`, whose default mode is this brief, or reads the
resource `chronicle://brief/{project}`. A project's brief shows its own decisions
only, and its own facts before at most four facts recorded without a project.


## Web view

The HTTP server also serves a read-only view of the memory for people, at
`http://127.0.0.1:8093/ui`:
- `/ui`: process health (event-loop lag, last write, unindexed events, last
  scheduled runs), the last good backup, warnings, open tasks, projects active in
  the last 30 days, the latest events;
- `/ui/project/{project}`: its open tasks, current facts and latest events;
- `/ui/task/{project}/{task_id}`: the latest checkpoint, earlier ones, the task's
  facts and events;
- `/ui/facts`: every current fact; `/ui/facts?slot=X`: every value that slot had,
  newest first, with what replaced it;
- `/ui/search?q=...&project=...`: recall, with how each hit matched;
- `/ui/event/{id}`, `/ui/note/{id}`: one record with its evidence and who recorded it.

Pages are rendered on the server with no script. Everything shown is escaped
text, and a Content-Security-Policy forbids scripts, frames and outside
requests. Only this machine is answered: the `Host` must be a loopback name, an
`Origin` must be the server's own, and a cross-site `Sec-Fetch-Site` is refused.
Quarantined events never appear.

### Atlas

`/ui/atlas` is the same memory as a map to wander through. Projects are suns,
tasks rings, events stars, notes pages, fact slots crystals; the colour is the
project, the brightness how recent. Lines are what the memory records (an event
in its task, a task in its project, a note linking another note, an event that
set a fact) and, as a separate layer, resemblance in meaning. A click flies to a
memory and opens it in full, with its links, backlinks and nearest memories by
meaning. The river at the bottom is the time machine: drag it to any day, or
play the memory growing; the facts known on that day are listed beside it.
Search with `⌘K` or `/`.

The page runs scripts, its own only: they come from the package, nothing from
another site, and its Content-Security-Policy allows nothing else (`script-src
'self'`, no inline code). Memory text is always set as text, never as markup.
The same machine-only checks apply, and the data comes from three JSON routes:
- `/ui/api/atlas`: nodes, recorded edges and every fact value with its time span;
- `/ui/api/atlas/similar`: edges by meaning (below);
- `/ui/api/item?id=event:<id>` or `note:<id>`: one memory in full.

Texts (an event's words, a note's title, a fact's value, a record opened in full)
are redacted again on the way out; names of projects, tasks and fact slots are
shown as recorded, as on every page. Quarantined events (the event, or every one
of its observations) and deleted notes are not on the map, in its records or in
its edges.

Edges by meaning link each event and note to its three nearest memories at a
cosine similarity of 0.80 or more (the active embedding model's vectors; a note
is the mean of its sections), and mark a pair at 0.995 or more as a likely
duplicate. The first pass compares every pair: about 40 seconds for 4,000
memories. It runs as its own process, started by the server when the page asks
and the stored vectors changed, and keeps its result in
`chronicle-artifacts/atlas/similar.json` (its log beside it). A later pass
compares only the memories added or changed since, so it takes seconds; the page
shows the previous edges meanwhile. `chronicle atlas similar` runs a pass by hand
(`--full` compares every pair again).


## Notes written on purpose

Agents often keep durable notes as files, such as Claude's file memory. With a
`[notes]` section in the manifest, Chronicle indexes them so every agent can
recall them (FR-10):

```toml
[notes]
paths = ["~/.claude/projects/*/memory/*.md"]  # absolute, or from ~
exclude = ["*.bak-*", "*/_archive/*"]
deny = ["*api-key*", "*keys.md"]  # never read, whatever they hold
sync_minutes = 15                  # the long-running server re-syncs this often
```

`chronicle notes sync` re-indexes the changed notes, keeps a tombstone for a
deleted one, and never writes to a note; the server runs the same sync every
`sync_minutes`.

- **Secrets.** The secret filter runs on each section of a note (text under one
  heading) as a whole, before a long section is cut into pieces: a key is never
  cut in two, and a pattern never spans two sections. Every other stored string
  is filtered too. Built-in name patterns such as `*secret*`, `*credential*`,
  `*private-key*`, `id_rsa*` and `*.pem` apply on top of `deny`. When the parser
  or the filter changes, every note is indexed again.
- **Files not read.** A file reached through a symbolic link below the fixed
  part of a pattern, a file whose path holds a likely secret, and a file that is
  not UTF-8 text are reported, not read. `**` never descends through a link,
  and wildcards skip hidden files and folders.
- **Removal.** A deleted or newly denied note keeps only its path as a
  tombstone. The sync compacts the full-text index, deletes with
  `secure_delete`, and empties the write-ahead log when no reader holds it;
  until then the log may keep an older copy.
- **Projects.** A note inside a project root, or in the file memory of a
  directory inside that root, belongs to that project. In the file memory of a
  directory under the workspace, it belongs to that workspace child's project.
  A note whose frontmatter names a project (`project: atlas`, any registered
  spelling) belongs to that project wherever it lies. Any other note is global.

`query_memory` returns notes in `notes`, next to events in `results`: the best
section of each note, with its heading, path and id. In a project scope, the
project's own notes come before global ones. `chronicle://note/{document_id}`
serves a whole note; `chronicle notes status` counts what the index holds.


## Durability

- **Writes:** SQLite WAL with full commit synchronization; events, observations,
  facts, and evidence links commit together.
- **Schema changes:** an empty database initialises on first use. An existing one is
  never upgraded as a side effect of connecting: run `chronicle migrate` or start the
  server with `chronicle-mcp --migrate`; both take an online backup
  (`chronicle.db.bak-premigrate-…`) under the same write lock as the upgrade. Without
  `--migrate` a server refuses to start on an outdated schema (exit code 3), and a
  read-only server never creates or changes a database. A database newer than the installed code, or
  another application's SQLite file, is refused. After installing a release with new
  migrations, migrate before scheduled jobs run: until then they exit with code 3.
  `CHRONICLE_AUTO_MIGRATE=1` restores upgrade-on-connect for the CLI and library;
  a server upgrades only with `--migrate`. `chronicle migrate --revert N` undoes the
  newest migration N with its down script (`migrations/down/`), after an online backup
  (`chronicle.db.bak-prerevert-…`); install the previous release before the server
  restarts, or `--migrate` applies N again. A migration that dropped data reverts from
  the copy taken before it: `--revert 18 --archive chronicle.db.bak-premigrate-v17-v18-…`,
  then `--revert 17 --archive chronicle.db.bak-premigrate-v16-v17-…` (one copy serves both
  when 0017 and 0018 came in one upgrade). Keep that copy outside the retention-pruned backup folders until the release has
  proved itself. `CHRONICLE_REQUIRE_ROOT=1`
  makes a process without `CHRONICLE_ROOT`/`CHRONICLE_MANIFEST` exit with code 2
  instead of falling back to `~/.max-chronicle`.
- **Read-only servers:** SQLite reads a WAL database through its `-wal` and `-shm`
  files, so even a read-only server creates them when they are missing. Next to a
  live writer they already exist. To serve a copy from a read-only directory,
  switch the copy to `PRAGMA journal_mode = DELETE` first.
- **Backups:** independent artifact copies, content-hash inventory, database
  integrity checks, and verification after relocation. Legacy backups disclose
  incomplete inventory coverage. Choose a separate backup device for disk failure.
  Explicitly purged artifacts are counted separately; they are not claimed as restorable.
  Retention keeps the newest backup of each of the last 14 days, 8 weeks and 12
  months that have one (`[backup_retention]` in the automation config:
  `daily`, `weekly`, `monthly`). Each backup reports what the policy would remove;
  removal happens only with `prune = true` or `chronicle backup-prune --apply`.
  A backup the disk prevented (not mounted, full, read-only) is `deferred` and
  retried the same day.
- **Scheduled jobs:** with `[scheduler] enabled = true` in the automation config,
  the HTTP server runs daily capture, daybook, weekly audit and backup itself,
  each as its own process, one at a time, and `chronicle launchd install` installs
  no job agents (launchd keeps only the server); job agents an earlier install left
  are unloaded (with `--load`) and moved to `retired-job-agents/` in the launchd
  runtime directory. A job's due moment is computed in
  the installation's timezone, and its run is keyed by that moment (its date, or
  for the weekly audit its ISO week): a run delayed past midnight fills the day it
  was due, never the next. A failed run is retried after 30 minutes, up to six
  times, and so is a job that fails before it records anything (a broken config);
  a deferred one (the backup disk not mounted) every 30 minutes until it runs; one
  still marked running after the stale TTL (`stale_run_ttl_hours`, six by default)
  is started again under the same limits; one due more than three days ago is
  skipped. A job runs in its own process group with a two-hour limit, and inherits
  the server's environment: a changed `env_file` reaches jobs after a server
  restart. `/health` shows
  the last runs under `scheduler`. `chronicle automation run --job X --occurrence
  ISO` runs a job as the occurrence it names.


## Mem0

Chronicle mirrored events into Mem0 until 0.26; migration 0018 removed the
mirror. A manifest or automation config from before may keep its Mem0 settings
(`[mem0]`, `paths.mem0_dump`, `paths.mem0_bridge`, `mem0_queries`,
`[jobs.mem0_dump]`, `mem0_sync_batch_size`): Chronicle ignores them and runs no
job for them. A launchd agent an earlier install left for `mem0-dump` shows in
`chronicle launchd doctor` as `retired_job_installed`; `chronicle launchd install`
takes it down. To keep what Mem0 held, export it as notes the index reads (FR-10).


## CLI

```bash
chronicle record "Selected local storage" --project demo --task-id ship \
  --agent agent-a --request-id decision-1 --category decision --why "Works offline"
chronicle record "Ready for review" --project demo --task-id ship \
  --checkpoint-file checkpoint.json --request-id handoff-1
chronicle startup --project demo --task-id ship --focus "Continue review" --format json
chronicle query-memory "local storage" --project demo --task-id ship --format json
chronicle eval --golden questions.jsonl --out results/baseline.json
chronicle timeline --at "2026-09-15T12:00:00Z"
chronicle timeline --at "2026-09-15T12:00:00Z" --as-of   # what was known then
chronicle backup --force
chronicle backup-prune            # what retention would remove; --apply removes it
chronicle doctor --summary        # server, jobs, last good backup, disks on one screen
```

`chronicle doctor` asks the HTTP server at `CHRONICLE_SERVER_URL`, or else at the
address the server reads (`MCP_HOST`, `MCP_PORT`, default port 8000); `--server-url`
overrides both. `--fact-file` accepts the assertion JSON above. Startup defaults to compact source
metadata; `--full` includes full source content. Run `chronicle --help` for optional
automation and maintenance commands.

