# Agent fact sheet for `chronicle connect` (stage 1)

Collected 2026-11-27 on one macOS machine. Each fact names its evidence:

- **file**: the agent's real user-level file on that machine, read only.
- **run**: the agent's own CLI, run with a temporary home (`HOME`, and the
  agent's own home variable where it has one); the output or the file it wrote.
- **code**: the agent's shipped code (bundle or binary) or its public source.
- **doc**: a documentation page fetched on that date.

A part with no evidence is "not confirmed" and `connect` does not build it.
Versions seen: Claude Code (native install), Codex CLI 0.172.0, ZCode CLI 0.18.0,
opencode 2.0.32, Gemini CLI 0.46.0. Cursor is not installed.

## Claude Code

| Item | Fact | Evidence |
|---|---|---|
| (a) MCP server | `<config>/.claude.json`, key `mcpServers.<name>` = `{"type": "http", "url": "<url>"}`. Two-space JSON, no comments; the file also holds prompt history and is rewritten by Claude Code. | run: `claude mcp add --scope user --transport http chronicle <url>` wrote exactly this object; file: same shape for the existing entries |
| (b) pre-approval | `<config>/.claude/settings.json` (`$CLAUDE_CONFIG_DIR/settings.json`), `permissions.allow`: `"mcp__<server>__<tool>"` per tool. The real file has `permissions.deny` and no `allow`. | doc: https://code.claude.com/docs/en/permissions ("To match one tool, write its full name, `mcp__<server>__<tool>`"); file |
| (c) session start | same `settings.json`, `hooks.SessionStart` = `[{"matcher": "startup\|resume\|clear\|compact", "hooks": [{"type": "command", "command": "...", "timeout": 5}]}]`; a `UserPromptSubmit` group without matcher gives the brief when SessionStart did not. Plain stdout of exit 0 is added as context. | file (both events in use with a brief hook); doc: https://code.claude.com/docs/en/hooks |
| (d) instructions | `~/.claude/CLAUDE.md` (`$CLAUDE_CONFIG_DIR/CLAUDE.md`) | file; doc: https://code.claude.com/docs/en/memory |
| (e) home variables | `CLAUDE_CONFIG_DIR`: `.claude.json` moves to `$CLAUDE_CONFIG_DIR/.claude.json` and the settings directory is `$CLAUDE_CONFIG_DIR`; otherwise `$HOME/.claude.json` and `$HOME/.claude/`. | run: `claude mcp add` with `CLAUDE_CONFIG_DIR` set wrote `$CLAUDE_CONFIG_DIR/.claude.json` |

## Codex CLI

| Item | Fact | Evidence |
|---|---|---|
| (a) MCP server | `$CODEX_HOME/config.toml`, table `[mcp_servers.<name>]` with `url = "<url>"` (streamable HTTP). | run: `codex mcp add --help` ("`--url` URL for a streamable HTTP MCP server"); file: `[mcp_servers.chronicle]` with `url` only |
| (b) pre-approval | `[mcp_servers.<name>.tools.<tool>]` with `approval_mode = "approve"` (or `default_tools_approval_mode` on the server). Values `auto`, `prompt`, `writes`, `approve`; `approve` never asks. | run: a temporary config with a bad value fails with "unknown variant ..., expected one of `auto`, `prompt`, `writes`, `approve` in `mcp_servers.chronicle.tools.query_memory.approval_mode`"; code: openai/codex `codex-rs/core/src/mcp_tool_call.rs` at 045a11fe, `requires_mcp_tool_approval_for_mode`: `AppToolApproval::Approve => false`; doc: https://learn.chatgpt.com/docs/config-file/config-reference |
| (c) session start | `$CODEX_HOME/hooks.json` = `{"hooks": {"SessionStart": [{"matcher": "startup\|resume\|clear\|compact", "hooks": [{"type": "command", "command": "...", "timeout": 5}]}]}}`. Plain stdout becomes context; about 2,500 tokens per hook output are kept, so the brief budget is 4,500 characters. A new or changed hook runs only after the user trusts it in `/hooks`: Codex stores `[hooks.state."<hooks.json path>:session_start:<group>:<hook>"] trusted_hash` in `config.toml`. `connect` does not write that trust. | file: `hooks.json` and the `hooks.state` table; doc: https://learn.chatgpt.com/docs/hooks |
| (d) instructions | `$CODEX_HOME/AGENTS.md`; `AGENTS.override.md` is read instead when it exists and is not empty. | file; doc: https://learn.chatgpt.com/docs/agent-configuration/agents-md |
| (e) home variables | `CODEX_HOME` (default `~/.codex`). | run: `codex mcp get` with `CODEX_HOME` set read the temporary `config.toml` |

## ZCode CLI 0.18

| Item | Fact | Evidence |
|---|---|---|
| (a) MCP server | `$HOME/.zcode/cli/config.json`, key `mcp.servers.<name>` = `{"type": "http", "url": "<url>"}` (`timeoutMs` optional). | file; code: the bundle's config merge reads `mcp.servers` |
| (b) pre-approval | same file, `permission.allowedTools`: `"mcp__<server>__<tool>"`. | file (an existing list of six `mcp__chronicle__*` names); code: `PermissionAllowedTools: "permission.allowedTools"`, tool names built as `` `mcp__${server}__${tool}` `` |
| (c) session start | same file, `hooks` = `{"enabled": true, "events": {"SessionStart": [{"matcher": "startup", "hooks": [{"type": "command", "command": "...", "timeout": 5}]}]}}`. Hooks run only when some config sets `hooks.enabled: true`. Stdout is read as JSON only when it starts with `{`: `{"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": "..."}}`. **Runs in headless `--prompt` mode.** | run: `zcode --prompt` with a temporary `HOME` and `ZCODE_DATA_BASE_DIR` ran a SessionStart hook from `$HOME/.zcode/cli/config.json` (its stdin: `cwd`, `hook_event_name`, `session_id`, `source: "startup"`; log phase `session_start_hooks` completed); code: hook schema `enabled`, `timeoutMs`, `maxOutputBytes`, `events.SessionStart`, output schema with `hookSpecificOutput.additionalContext` |
| (d) instructions | `$HOME/.zcode/AGENTS.md` (`HOME`, else `USERPROFILE`). | file; code: `join(HOME, ".zcode", "AGENTS.md")` |
| (e) home variables | The user config is `join(homedir(), ".zcode", "cli", "config.json")`: only `HOME` moves it. `ZCODE_DATA_BASE_DIR` moves plugins and data, not this file. | run: with `ZCODE_DATA_BASE_DIR` set and the config only there, the log said `configSourceUser: false`; with the config under `$HOME`, `configSourceUser: true`; code: `userConfigPath ?? join(homeDir, ".zcode", "cli", "config.json")` |

## opencode 2.0.32

| Item | Fact | Evidence |
|---|---|---|
| (a) MCP server | `$XDG_CONFIG_HOME/opencode/opencode.jsonc` (or `opencode.json`), key `mcp.servers.<name>` = `{"type": "remote", "url": "<url>"}`. JSONC with comments and inline objects. | run: `opencode mcp add chronicle --url <url> --global` wrote exactly this into a temporary `opencode.jsonc` and kept its comment; code: config schema https://opencode.ai/config.json `Mcp.RemoteConfigEncoded` |
| (b) pre-approval | `permissions` (array): `{"action": "<server>_<tool>", "resource": "*", "effect": "allow"}`. The deprecated `permission` object is turned into rules too, and the lists are joined as `tools`, `permission`, `permissions`; the last matching rule wins; no match asks. | doc: https://opencode.ai/docs/permissions/ (action `<server>_<tool>`, resource `*` for an MCP tool; no match is `ask`); code: the binary joins `[...tools, ...permission, ...permissions]` and picks `findLast` |
| (c) session start | **not confirmed**: the plugin types have no session-start event. The block tells the agent to start with `startup_bundle`. | code: `@opencode-ai/plugin` types (checked by the lead) |
| (d) instructions | `$XDG_CONFIG_HOME/opencode/AGENTS.md`, always loaded; without it opencode reads `~/.claude/CLAUDE.md` instead, so `connect` does not create it. | doc: https://opencode.ai/docs/rules/; file (also listed under `instructions`) |
| (e) home variables | `XDG_CONFIG_HOME` (default `~/.config`). | run: `opencode mcp add --global` with `XDG_CONFIG_HOME` set wrote `$XDG_CONFIG_HOME/opencode/opencode.json`; `opencode debug paths` |

## Gemini CLI 0.46

| Item | Fact | Evidence |
|---|---|---|
| (a) MCP server | `$GEMINI_CLI_HOME/.gemini/settings.json` (default `$HOME/.gemini/settings.json`), key `mcpServers.<name>` = `{"url": "<url>", "type": "http"}`. | run: `gemini mcp add --scope user --transport http --trust chronicle <url>` wrote `{"url": ..., "type": "http", "trust": true}` |
| (b) pre-approval | `"trust": true` in the server's entry: every tool of that server runs without confirmation. | run: as above; `--trust` help: "bypass all tool call confirmation prompts" |
| (c) session start | same file, `hooks.SessionStart` = `[{"matcher": "*", "hooks": [{"name": "...", "type": "command", "command": "...", "timeout": 5000}]}]` (timeout in milliseconds). `hooksConfig.enabled` defaults to true. Output is JSON: `hookSpecificOutput.additionalContext`. | code: bundle settings schema (`hooks.SessionStart`, `HookDefinitionArray`, `hooksConfig.enabled` default `true`), `fireSessionStartEvent`, `getAdditionalContext()` |
| (d) instructions | `$GEMINI_CLI_HOME/.gemini/GEMINI.md`. | file; code: `DEFAULT_CONTEXT_FILENAME = "GEMINI.md"`, `getGlobalGeminiDir()` = `join(homedir(), ".gemini")` |
| (e) home variables | `GEMINI_CLI_HOME` replaces the home directory; `.gemini` is under it. | code: `homedir() { return process.env["GEMINI_CLI_HOME"] \|\| os.homedir() }`; `getGlobalSettingsPath()` = `join(getGlobalGeminiDir(), "settings.json")` |

## Cursor

Not installed: `connect cursor` only prints. `~/.cursor/mcp.json`,
`{"mcpServers": {"<name>": {"url": "<url>"}}}` (doc: https://cursor.com/docs/context/mcp).
No global instruction file is documented there; the block is printed for manual use.

## Where the lead's matrix differs from what was found

- opencode permissions: the real file uses the deprecated `permission` object; the
  matrix names only `permissions`. Both are read (joined in that order).
- Gemini CLI: the matrix gives `httpUrl`; `gemini mcp add --transport http` 0.46 writes
  `url` with `type: "http"`. The CLI's own output is used.
- ZCode: the matrix leaves headless hooks open; the run above shows a SessionStart hook
  in `--prompt` mode. The matrix calls config-home variables unknown; only `HOME`
  moves the user config.
- Codex: the matrix leaves the meaning of `approve` open; the source defines it.
- Config-home variables for opencode (`XDG_CONFIG_HOME`) and Gemini CLI
  (`GEMINI_CLI_HOME`) were found in their code and runs.
