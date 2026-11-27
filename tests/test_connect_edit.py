"""The config editing core of `chronicle connect`: one contiguous piece per change, every other byte kept."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tomllib

import pytest

from max_chronicle.connect_edit import (
    EditRefused,
    JsonDoc,
    Ledger,
    Loaded,
    TextDoc,
    TomlDoc,
    load,
    sha256_bytes,
    write,
)

CLAUDE_LIKE = """{
  "numStartups": 3,
  "tipsHistory": {
    "a": 1
  },
  "history": [
    {"display": "fix \\"mcpServers\\": { here", "pastedContents": {}}
  ],
  "mcpServers": {
    "playwright": {
      "type": "stdio",
      "command": "npx",
      "args": [
        "@playwright/mcp@latest"
      ]
    }
  },
  "userID": "example-user"
}
"""

JSONC_LIKE = """{
  "$schema": "https://opencode.ai/config.json",
  // A comment with a "quoted": value and a // inside.
  "update": "disable",
  "compaction": { "buffer": 0 },
  /* a block
     comment */
  "permission": {
    "bash": { "*": "allow" },
    "doom_loop": "allow" // trailing
  },
  "instructions": [
    "~/.config/opencode/AGENTS.md"
  ]
}
"""

CODEX_LIKE = """model = "example-model"
approval_policy = "on-request"

[mcp_servers.node_repl]
command = "/opt/example/node_repl"
args = ["--flag"]
startup_timeout_sec = 120

[mcp_servers.node_repl.env]
EXAMPLE = "1"

# A comment that belongs to the next table.
[projects."/home/dev/projects/demo"]
trust_level = "trusted"

[features]
multi_agent = true
"""


def _file(tmp_path: Path, name: str, text: str, mode: int = 0o644) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    os.chmod(path, mode)
    return path


def _outside_kept(before: str, after: str, start: int, old: str, new: str) -> bool:
    """The text after the edit is the text before with one piece changed at *start*."""
    return after[:start] == before[:start] and after[start + len(new):] == before[start + len(old):]


def _apply(edit) -> str:
    write(edit)
    return edit.path.read_text()


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


def test_a_member_is_added_as_one_piece_and_nothing_else_changes(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE, 0o600)
    doc = JsonDoc(load(path), comments=False)

    edit = doc.insert_member(["mcpServers"], "chronicle", {"type": "http", "url": "http://127.0.0.1:4100/mcp"})
    after = _apply(edit)

    assert edit.old == "" and _outside_kept(CLAUDE_LIKE, after, edit.start, "", edit.new)
    assert json.loads(after)["mcpServers"]["chronicle"]["url"] == "http://127.0.0.1:4100/mcp"
    assert '    "chronicle": {\n      "type": "http",\n      "url": "http://127.0.0.1:4100/mcp"\n    }' in after
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_undoing_an_added_member_restores_every_byte(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE)
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})
    _apply(edit)
    change = {"op": edit.op, "piece": edit.new, "original": edit.old, "offset": edit.start, "logical": edit.logical}

    undo = JsonDoc(load(path), comments=False).undo(change)

    assert _apply(undo).encode() == CLAUDE_LIKE.encode()


def test_a_string_that_looks_like_a_key_is_left_alone(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE)
    doc = JsonDoc(load(path), comments=False)

    edit = doc.insert_member(["mcpServers"], "chronicle", {"url": "u"})

    assert edit.start > CLAUDE_LIKE.index('"mcpServers": {\n')  # the real key, not the one in history


def test_missing_objects_on_the_path_are_created_in_the_same_piece(tmp_path) -> None:
    path = _file(tmp_path, "settings.json", '{\n  "model": "x"\n}\n')
    doc = JsonDoc(load(path), comments=False)

    edit = doc.append_items(["hooks", "SessionStart"], [{"hooks": [{"type": "command", "command": "c"}]}])
    after = _apply(edit)

    assert json.loads(after)["hooks"]["SessionStart"][0]["hooks"][0]["command"] == "c"
    assert edit.logical["created"] == ["hooks"]
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical}
    assert _apply(JsonDoc(load(path), comments=False).undo(change)) == '{\n  "model": "x"\n}\n'


def test_an_empty_object_gets_its_first_member(tmp_path) -> None:
    path = _file(tmp_path, "a.json", '{\n  "mcpServers": {}\n}\n')
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})

    after = _apply(edit)

    assert after == '{\n  "mcpServers": {\n    "chronicle": {\n      "url": "u"\n    }\n  }\n}\n'


def test_a_missing_file_is_created_and_its_undo_deletes_it(tmp_path) -> None:
    path = tmp_path / "hooks.json"
    edit = JsonDoc(load(path), comments=False).append_items(["hooks", "SessionStart"], [{"matcher": "startup"}])

    assert edit.op == "create"
    write(edit)
    assert json.loads(path.read_text()) == {"hooks": {"SessionStart": [{"matcher": "startup"}]}}
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": 0, "logical": edit.logical}
    write(JsonDoc(load(path), comments=False).undo(change))
    assert not path.exists()


def test_items_are_appended_to_an_array(tmp_path) -> None:
    text = '{\n  "permissions": {\n    "deny": [\n      "Read(~/.ssh/**)"\n    ],\n    "allow": [\n      "Bash(ls)"\n    ]\n  }\n}\n'
    path = _file(tmp_path, "s.json", text)

    edit = JsonDoc(load(path), comments=False).append_items(["permissions", "allow"], ["mcp__chronicle__a", "mcp__chronicle__b"])
    after = _apply(edit)

    assert json.loads(after)["permissions"]["allow"] == ["Bash(ls)", "mcp__chronicle__a", "mcp__chronicle__b"]
    assert '      "Bash(ls)",\n      "mcp__chronicle__a",\n      "mcp__chronicle__b"\n    ]' in after
    assert _outside_kept(text, after, edit.start, "", edit.new)


def test_jsonc_comments_and_urls_survive(tmp_path) -> None:
    path = _file(tmp_path, "opencode.jsonc", JSONC_LIKE)
    doc = JsonDoc(load(path), comments=True)

    edit = doc.insert_member(["mcp", "servers"], "chronicle", {"type": "remote", "url": "http://127.0.0.1:4100/mcp"})
    after = _apply(edit)

    assert "// A comment with a \"quoted\": value and a // inside." in after
    assert "/* a block\n     comment */" in after
    assert _outside_kept(JSONC_LIKE, after, edit.start, "", edit.new)
    assert JsonDoc(load(path), comments=True).get(["$schema"]) == "https://opencode.ai/config.json"


def test_an_entry_after_a_trailing_comment_goes_first(tmp_path) -> None:
    path = _file(tmp_path, "opencode.jsonc", JSONC_LIKE)

    edit = JsonDoc(load(path), comments=True).insert_member(["permission"], "chronicle_query_memory", "allow")
    after = _apply(edit)

    assert '"doom_loop": "allow" // trailing\n' in after
    assert '  "permission": {\n    "chronicle_query_memory": "allow",\n    "bash"' in after


def test_an_inline_object_stays_inline(tmp_path) -> None:
    path = _file(tmp_path, "opencode.jsonc", JSONC_LIKE)

    after = _apply(JsonDoc(load(path), comments=True).insert_member(["compaction"], "reserved", 1))

    assert '"compaction": { "buffer": 0, "reserved": 1 },' in after


def test_tabs_are_kept_as_the_indent(tmp_path) -> None:
    path = _file(tmp_path, "t.json", '{\n\t"mcpServers": {\n\t\t"a": {\n\t\t\t"url": "x"\n\t\t}\n\t}\n}\n')

    after = _apply(JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"}))

    assert '\t\t"chronicle": {\n\t\t\t"url": "u"\n\t\t}\n' in after


def test_non_ascii_text_is_kept_byte_for_byte(tmp_path) -> None:
    text = '{\n  "заметка": "Память агента",\n  "mcpServers": {}\n}\n'
    path = _file(tmp_path, "u.json", text)

    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})
    after = _apply(edit)

    assert after.startswith('{\n  "заметка": "Память агента",')


def test_a_value_is_replaced_in_place(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE)
    doc = JsonDoc(load(path), comments=False)
    edit = doc.insert_member(["mcpServers"], "chronicle", {"type": "http", "url": "http://127.0.0.1:4100/mcp"})
    _apply(edit)

    replace = JsonDoc(load(path), comments=False).replace_value(["mcpServers", "chronicle", "url"],
                                                                "http://127.0.0.1:4100/mcp/read")
    before = path.read_text()
    after = _apply(replace)

    assert _outside_kept(before, after, replace.start, replace.old, replace.new)
    change = {"op": replace.op, "piece": replace.new, "original": replace.old, "offset": replace.start,
              "logical": replace.logical}
    assert _apply(JsonDoc(load(path), comments=False).undo(change)) == before


def test_a_key_is_renamed_and_renamed_back(tmp_path) -> None:
    text = '{\n  "mcpServers": {\n    "Память": {\n      "url": "http://127.0.0.1:4100/mcp"\n    }\n  }\n}\n'
    path = _file(tmp_path, "z.json", text)

    edit = JsonDoc(load(path), comments=False).rename_key(["mcpServers"], "Память", "chronicle")
    after = _apply(edit)

    assert '"chronicle": {' in after and "Память" not in after
    change = {"op": edit.op, "piece": edit.new, "original": edit.old, "offset": edit.start, "logical": edit.logical}
    assert _apply(JsonDoc(load(path), comments=False).undo(change)) == text


def test_an_undo_finds_the_piece_after_other_edits_elsewhere(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE)
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})
    _apply(edit)
    # The owner rewrites another value, which moves the piece.
    path.write_text(path.read_text().replace('"numStartups": 3', '"numStartups": 1234'))
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical}

    after = _apply(JsonDoc(load(path), comments=False).undo(change))

    assert after == CLAUDE_LIKE.replace('"numStartups": 3', '"numStartups": 1234')


def test_an_undo_of_an_entry_changed_by_hand_is_refused_with_steps(tmp_path) -> None:
    path = _file(tmp_path, "claude.json", CLAUDE_LIKE)
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})
    _apply(edit)
    path.write_text(path.read_text().replace('"url": "u"', '"url": "changed"'))
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical}

    with pytest.raises(EditRefused) as refused:
        JsonDoc(load(path), comments=False).undo(change)

    assert "Remove the key 'chronicle'" in refused.value.manual


def test_duplicate_keys_on_the_path_are_refused(tmp_path) -> None:
    path = _file(tmp_path, "d.json", '{\n  "mcpServers": {},\n  "mcpServers": {}\n}\n')

    with pytest.raises(EditRefused, match="duplicate key 'mcpServers'"):
        JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {})


def test_duplicate_keys_off_the_path_do_not_matter(tmp_path) -> None:
    path = _file(tmp_path, "d.json", '{\n  "other": {"a": 1, "a": 2},\n  "mcpServers": {}\n}\n')

    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})

    assert '"other": {"a": 1, "a": 2}' in edit.after_text


@pytest.mark.parametrize("text", [
    '{\n  "a": 1,\n}\n',               # trailing comma
    "{\n  'a': 1\n}\n",               # single quotes
    '{\n  a: 1\n}\n',                  # unquoted key
    '{\n  "a": 0x10\n}\n',             # hex number
    '{\n  "a": Infinity\n}\n',
])
def test_json5_syntax_is_refused(tmp_path, text) -> None:
    path = _file(tmp_path, "j.jsonc", text)

    with pytest.raises(EditRefused, match="cannot be read as JSONC"):
        JsonDoc(load(path), comments=True)


def test_comments_in_strict_json_are_refused(tmp_path) -> None:
    path = _file(tmp_path, "j.json", '{\n  // no\n  "a": 1\n}\n')

    with pytest.raises(EditRefused, match="comments in a file that must be strict JSON"):
        JsonDoc(load(path), comments=False)


def test_a_change_that_would_not_parse_is_refused_before_writing(tmp_path, monkeypatch) -> None:
    path = _file(tmp_path, "a.json", '{\n  "mcpServers": {}\n}\n')
    monkeypatch.setattr("max_chronicle.connect_edit._render_member", lambda *args: '"broken": ')

    with pytest.raises(EditRefused, match="would not parse"):
        JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {})
    assert path.read_text() == '{\n  "mcpServers": {}\n}\n'


# ---------------------------------------------------------------------------
# Reading and writing files
# ---------------------------------------------------------------------------


def test_a_symbolic_link_is_refused(tmp_path) -> None:
    target = _file(tmp_path, "real.json", "{}\n")
    link = tmp_path / "link.json"
    link.symlink_to(target)

    with pytest.raises(EditRefused, match="symbolic link") as refused:
        load(link)
    assert "real.json" in refused.value.manual


@pytest.mark.parametrize("raw, reason", [
    (b"\xef\xbb\xbf{}\n", "byte-order mark"),
    (b'{\r\n  "a": 1\r\n}\r\n', "CRLF"),
    (b'{"a": "\xff"}\n', "not UTF-8"),
])
def test_files_that_would_be_rewritten_whole_are_refused(tmp_path, raw, reason) -> None:
    path = tmp_path / "f.json"
    path.write_bytes(raw)

    with pytest.raises(EditRefused, match=reason):
        load(path)
    assert path.read_bytes() == raw


def test_a_write_keeps_the_mode_and_takes_a_backup(tmp_path) -> None:
    path = _file(tmp_path, "c.json", CLAUDE_LIKE, 0o600)
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})

    result = write(edit)

    backup = Path(result.backup)
    assert backup.name.startswith("c.json.bak-") and backup.read_bytes() == CLAUDE_LIKE.encode()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600 and stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert result.sha256_before == sha256_bytes(CLAUDE_LIKE.encode())
    assert result.sha256_after == sha256_bytes(path.read_bytes())
    assert not list(tmp_path.glob(".c.json.*"))  # no temporary file left


def test_a_write_is_refused_when_the_file_changed_after_it_was_read(tmp_path) -> None:
    path = _file(tmp_path, "c.json", CLAUDE_LIKE)
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})
    path.write_text(CLAUDE_LIKE.replace("3", "4"))

    with pytest.raises(EditRefused, match="changed on disk"):
        write(edit)
    assert path.read_text() == CLAUDE_LIKE.replace("3", "4")
    assert not list(tmp_path.glob("c.json.bak-*"))


def test_two_backups_in_one_second_do_not_overwrite_each_other(tmp_path) -> None:
    from datetime import datetime

    path = _file(tmp_path, "c.json", '{\n  "a": {}\n}\n')
    moment = datetime(2026, 11, 27, 12, 0, 0)
    write(JsonDoc(load(path), comments=False).insert_member(["a"], "x", 1), now=moment)
    write(JsonDoc(load(path), comments=False).insert_member(["a"], "y", 2), now=moment)

    assert sorted(item.name for item in tmp_path.glob("c.json.bak-*")) == [
        "c.json.bak-20261127-120000", "c.json.bak-20261127-120000-1"]


# ---------------------------------------------------------------------------
# TOML
# ---------------------------------------------------------------------------


def test_a_table_is_appended_and_every_other_byte_kept(tmp_path) -> None:
    path = _file(tmp_path, "config.toml", CODEX_LIKE, 0o600)
    doc = TomlDoc(load(path))

    edit = doc.append('[mcp_servers.chronicle]\nurl = "http://127.0.0.1:4100/mcp"\n')
    after = _apply(edit)

    assert after == CODEX_LIKE + '\n[mcp_servers.chronicle]\nurl = "http://127.0.0.1:4100/mcp"\n'
    assert tomllib.loads(after)["mcp_servers"]["chronicle"] == {"url": "http://127.0.0.1:4100/mcp"}


def test_undoing_appended_tables_restores_every_byte(tmp_path) -> None:
    path = _file(tmp_path, "config.toml", CODEX_LIKE.rstrip("\n"))  # no final newline
    first = TomlDoc(load(path)).append('[mcp_servers.chronicle]\nurl = "u"\n')
    _apply(first)
    second = TomlDoc(load(path)).append('[mcp_servers.chronicle.tools.query_memory]\napproval_mode = "approve"\n')
    _apply(second)

    for edit in (second, first):
        change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical}
        _apply(TomlDoc(load(path)).undo(change))

    assert path.read_text() == CODEX_LIKE.rstrip("\n")


@pytest.mark.parametrize("text", [
    '[mcp_servers]\nchronicle.url = "http://elsewhere/mcp"\n',
    'mcp_servers = { chronicle = { url = "http://elsewhere/mcp" } }\n',
    'mcp_servers.chronicle.url = "http://elsewhere/mcp"\n',
])
def test_a_dotted_key_or_an_inline_table_is_refused(tmp_path, text) -> None:
    path = _file(tmp_path, "config.toml", text)

    with pytest.raises(EditRefused, match="dotted key or an inline table"):
        TomlDoc(load(path)).replace_block(("mcp_servers", "chronicle"), '[mcp_servers.chronicle]\nurl = "u"\n')


def test_appending_under_an_inline_parent_is_refused(tmp_path) -> None:
    path = _file(tmp_path, "config.toml", 'mcp_servers = { other = { url = "x" } }\n')

    with pytest.raises(EditRefused, match="dotted key or an inline table"):
        TomlDoc(load(path)).append('[mcp_servers.chronicle]\nurl = "u"\n')


def test_a_table_that_exists_is_not_appended_again(tmp_path) -> None:
    path = _file(tmp_path, "config.toml", '[mcp_servers.chronicle]\nurl = "x"\n')

    with pytest.raises(EditRefused):
        TomlDoc(load(path)).append('[mcp_servers.chronicle]\nurl = "u"\n')


def test_a_table_block_is_replaced_and_the_next_comment_kept(tmp_path) -> None:
    text = CODEX_LIKE.replace("[features]", '[mcp_servers.chronicle]\nurl = "http://127.0.0.1:9/mcp"\n# about features\n\n[features]')
    path = _file(tmp_path, "config.toml", text)

    edit = TomlDoc(load(path)).replace_block(("mcp_servers", "chronicle"), '[mcp_servers.chronicle]\nurl = "u"\n')
    after = _apply(edit)

    assert '[mcp_servers.chronicle]\nurl = "u"\n# about features\n\n[features]' in after
    change = {"op": edit.op, "piece": edit.new, "original": edit.old, "offset": edit.start, "logical": edit.logical}
    assert _apply(TomlDoc(load(path)).undo(change)) == text


def test_a_table_is_renamed_by_its_header(tmp_path) -> None:
    text = '[mcp_servers.memory]\nurl = "http://127.0.0.1:4100/mcp"\n'
    path = _file(tmp_path, "config.toml", text)

    edit = TomlDoc(load(path)).rename_table(("mcp_servers", "memory"), ("mcp_servers", "chronicle"))
    after = _apply(edit)

    assert after == '[mcp_servers.chronicle]\nurl = "http://127.0.0.1:4100/mcp"\n'
    change = {"op": edit.op, "piece": edit.new, "original": edit.old, "offset": edit.start, "logical": edit.logical}
    assert _apply(TomlDoc(load(path)).undo(change)) == text


# ---------------------------------------------------------------------------
# Text blocks
# ---------------------------------------------------------------------------

BLOCK = "<!-- chronicle:begin role=write -->\n## Chronicle\n- text\n<!-- chronicle:end -->\n"


def test_a_block_is_appended_and_taken_out(tmp_path) -> None:
    path = _file(tmp_path, "CLAUDE.md", "# Rules\n\n- one\n")
    edit = TextDoc(load(path)).append_block(BLOCK, logical={"begin": "<!-- chronicle:begin", "end": "<!-- chronicle:end"})
    after = _apply(edit)

    assert after == "# Rules\n\n- one\n\n" + BLOCK
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical,
              "separator": "\n"}
    assert _apply(TextDoc(load(path)).undo(change)) == "# Rules\n\n- one\n"


def test_a_block_edited_by_hand_is_still_removed_by_its_markers(tmp_path) -> None:
    path = _file(tmp_path, "AGENTS.md", "# Rules\n")
    edit = TextDoc(load(path)).append_block(BLOCK, logical={"begin": "<!-- chronicle:begin", "end": "<!-- chronicle:end"})
    _apply(edit)
    path.write_text(path.read_text().replace("- text", "- text edited by hand") + "\nAfter.\n")
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": edit.start, "logical": edit.logical,
              "separator": "\n"}

    after = _apply(TextDoc(load(path)).undo(change))

    assert after == "# Rules\n\nAfter.\n"


def test_a_block_in_a_new_file_goes_with_the_file(tmp_path) -> None:
    path = tmp_path / "GEMINI.md"
    edit = TextDoc(load(path)).append_block(BLOCK, logical={"begin": "<!-- chronicle:begin", "end": "<!-- chronicle:end"})
    write(edit)
    change = {"op": edit.op, "piece": edit.new, "original": "", "offset": 0, "logical": edit.logical}

    write(TextDoc(load(path)).undo(change))

    assert not path.exists()


def test_an_old_block_is_found_and_replaced(tmp_path) -> None:
    path = _file(tmp_path, "AGENTS.md", "# Rules\n\n" + BLOCK + "\nEnd.\n")
    doc = TextDoc(load(path))
    span = doc.find_block("<!-- chronicle:begin", "<!-- chronicle:end")

    edit = doc.replace_block(span, BLOCK.replace("role=write", "role=read"))

    assert edit.after_text == "# Rules\n\n" + BLOCK.replace("role=write", "role=read") + "\nEnd.\n"


# ---------------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------------


def test_the_ledger_records_a_change_before_the_file_is_written(tmp_path) -> None:
    path = _file(tmp_path, "c.json", CLAUDE_LIKE)
    ledger = Ledger(tmp_path / "ws" / "connect-ledger.json")
    edit = JsonDoc(load(path), comments=False).insert_member(["mcpServers"], "chronicle", {"url": "u"})

    change = ledger.begin(edit, agent="claude", part="server")
    on_disk = json.loads((tmp_path / "ws" / "connect-ledger.json").read_text())

    assert on_disk["changes"][0]["status"] == "pending"
    assert on_disk["changes"][0]["sha256_before"] == sha256_bytes(CLAUDE_LIKE.encode())
    assert on_disk["changes"][0]["sha256_after"] == edit.sha256_after
    assert path.read_text() == CLAUDE_LIKE
    ledger.finish(change, write(edit))
    reread = Ledger(tmp_path / "ws" / "connect-ledger.json")
    assert reread.active("claude")[0]["sha256_after"] == sha256_bytes(path.read_bytes())
    assert stat.S_IMODE((tmp_path / "ws" / "connect-ledger.json").stat().st_mode) == 0o600


def test_a_ledger_in_an_unknown_format_is_refused(tmp_path) -> None:
    (tmp_path / "connect-ledger.json").write_text('{"version": 99}')

    with pytest.raises(EditRefused, match="unknown format"):
        Ledger(tmp_path / "connect-ledger.json")


def test_loaded_reports_a_missing_file() -> None:
    loaded = Loaded(Path("/nonexistent/x.json"), None, "", None)

    assert not loaded.exists and loaded.sha256 is None
