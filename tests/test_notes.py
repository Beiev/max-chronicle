"""The note index (FR-10, AC-14): notes written on purpose, recalled by every agent, secrets never stored."""

from __future__ import annotations

import asyncio
import base64
import codecs
import contextlib
from dataclasses import replace
import json
import logging
import os
import secrets
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from max_chronicle.browse import render_search_results
from max_chronicle.embeddings import active_profile, unpack_vector
from max_chronicle.evals import GoldenCase, run_eval
from max_chronicle.mcp_server import _start_notes_sync, build_server
from max_chronicle.notes import (
    CHUNK_CHARS,
    NOTE_INDEX_VERSION,
    _directory_slug,
    _slug_directory,
    chunk_note,
    discover,
    document_id,
    note_project,
    note_settings,
    read_note,
    sync_notes,
)
from max_chronicle.identity import Registry
from max_chronicle.recall import query_memory
from max_chronicle.store import config_from_manifest, open_connection

from max_chronicle import cli, service
from max_chronicle import mcp_server as mcp_server_module
from max_chronicle import notes as notes_module


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _note(path: Path, body: str, **fields: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "".join(f"{key}: {value}\n" for key, value in fields.items())
    path.write_text((f"---\n{header}---\n" if fields else "") + body, encoding="utf-8")
    return path


@pytest.fixture()
def notes(loaded_manifest, tmp_path):
    """A workspace with Claude file memory: a global note and notes of two projects."""
    workspace = Path(loaded_manifest["paths"]["workspace_root"])
    claude = tmp_path / "claude" / "projects"
    orbit = tmp_path / "code" / "orbit"
    manifest = {
        **loaded_manifest,
        "projects": [{"id": "orbit", "roots": [str(orbit)]}],
        "notes": {"paths": [str(claude / "*" / "memory" / "*.md"), str(orbit / "*.md")],
                  "exclude": ["*.bak-*", "*/_archive/*"]},
    }
    memory = claude / _directory_slug(workspace) / "memory"
    files = {
        "global": _note(memory / "cache-rule.md",
                        "# Release cache rule\n\n## Rule\nClear the build cache ahead of every release, always.\n\n"
                        "## Why\nA stale cache broke the identity of the deploy pipeline renders.\n",
                        name="Release cache rule", description="A rule about the build cache", type="feedback"),
        "atlas": _note(claude / (_directory_slug(workspace) + "-atlas") / "memory" / "deploy.md",
                       "## Deploy pipeline\nThe atlas deploy pipeline pushes to staging first.\n", name="Atlas deploy"),
        "orbit": _note(claude / _directory_slug(orbit) / "memory" / "orbit.md",
                       "## Launch\nOrbit launches on Tuesdays.\n", name="Orbit launch"),
        "inside": _note(orbit / "NOTES.md", "# Orbit notes\n\nThe orbit deploy pipeline has no staging.\n"),
        "backup": _note(memory / "cache-rule.md.bak-20260924.md", "old copy"),
        "archived": _note(memory / "_archive" / "old.md", "archived deploy pipeline note"),
    }
    return manifest, files


def _documents(manifest: dict) -> dict[str, dict]:
    with open_connection(config_from_manifest(manifest)) as connection:
        return {row["path"]: dict(row) for row in connection.execute("SELECT * FROM documents")}


def _chunks(manifest: dict, path: Path) -> list[tuple[str, str]]:
    with open_connection(config_from_manifest(manifest)) as connection:
        return [(row["heading"], row["text"]) for row in connection.execute(
            "SELECT c.heading, c.text FROM document_chunks c JOIN documents d ON d.id = c.document_id "
            "WHERE d.path = ? ORDER BY c.ordinal", (str(path),))]


def test_sync_indexes_notes_with_their_projects_titles_and_sections(notes) -> None:
    manifest, files = notes

    report = sync_notes(manifest)

    assert (report["indexed"], report["unchanged"], report["removed"]) == (4, 0, 0)
    documents = _documents(manifest)
    assert {Path(path).name: (row["project"], row["title"], row["kind"]) for path, row in documents.items()} == {
        "cache-rule.md": (None, "Release cache rule", "feedback"),
        "deploy.md": ("atlas", "Atlas deploy", None),
        "orbit.md": ("orbit", "Orbit launch", None),
        "NOTES.md": ("orbit", "Orbit notes", None),
    }
    assert _chunks(manifest, files["global"]) == [
        ("Release cache rule › Rule", "Clear the build cache ahead of every release, always."),
        ("Release cache rule › Why", "A stale cache broke the identity of the deploy pipeline renders."),
    ]


def test_an_unchanged_note_is_skipped_and_a_changed_one_reindexed(notes) -> None:
    manifest, files = notes
    sync_notes(manifest)
    assert (sync_notes(manifest)["indexed"], sync_notes(manifest)["unchanged"]) == (0, 4)

    _note(files["atlas"], "## Deploy pipeline\nThe atlas deploy pipeline now skips staging.\n", name="Atlas deploy")
    report = sync_notes(manifest)

    assert (report["indexed"], report["unchanged"]) == (1, 3)
    assert _chunks(manifest, files["atlas"]) == [("Atlas deploy › Deploy pipeline", "The atlas deploy pipeline now skips staging.")]
    assert not [note for note in query_memory(manifest, query="pushes staging first")["notes"]]


def test_a_deleted_note_keeps_a_tombstone_and_leaves_recall(notes) -> None:
    manifest, files = notes
    sync_notes(manifest)
    files["orbit"].unlink()

    assert sync_notes(manifest)["removed"] == 1
    row = _documents(manifest)[str(files["orbit"])]
    assert row["deleted_at_utc"] is not None and _chunks(manifest, files["orbit"]) == []
    assert query_memory(manifest, query="orbit launches tuesdays")["notes"] == []

    _note(files["orbit"], "## Launch\nOrbit launches on Tuesdays.\n", name="Orbit launch")
    assert sync_notes(manifest)["indexed"] == 1
    assert _documents(manifest)[str(files["orbit"])]["deleted_at_utc"] is None


def _synthetic_key() -> str:
    return "sk-" + "proj-" + secrets.token_urlsafe(36)


def test_a_note_with_a_key_is_found_and_the_key_is_never_stored(notes) -> None:
    """AC-14: after one sync another agent finds today's note; no stored text holds its key."""
    manifest, files = notes
    key = _synthetic_key()
    innocent = _note(files["global"].with_name("vendor-setup.md"),
                     f"## Vendor setup\nThe vendor sandbox rotates weekly. OPENAI_API_KEY={key}\n",
                     name="Vendor setup", description=f"uses {key}")
    denied = _note(files["global"].with_name("vendor-api-keys.md"), f"## Keys\nvendor sandbox {key}\n")

    report = sync_notes(manifest)

    assert report["denied"] == 1 and report["redactions"] >= 2
    assert str(denied) not in _documents(manifest)
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        stored = [json.dumps([tuple(row) for row in connection.execute(f"SELECT * FROM {table}")])
                  for table in ("documents", "document_chunks", "document_chunks_fts")]
    assert not any(key in text for text in stored)
    found = query_memory(manifest, query="vendor sandbox rotates")["notes"]
    assert [note["path"] for note in found] == [str(innocent)]
    assert key not in json.dumps(read_note(manifest, found[0]["document_id"]))


def test_project_notes_come_before_global_notes_in_a_project_scope(notes) -> None:
    manifest, files = notes
    sync_notes(manifest)

    atlas = [note["path"] for note in query_memory(manifest, query="deploy pipeline", project="atlas")["notes"]]
    orbit = [note["path"] for note in query_memory(manifest, query="deploy pipeline", project="orbit")["notes"]]
    anywhere = {note["path"] for note in query_memory(manifest, query="deploy pipeline")["notes"]}

    assert atlas == [str(files["atlas"]), str(files["global"])]
    assert orbit == [str(files["inside"]), str(files["global"])]
    assert anywhere == {str(files["atlas"]), str(files["global"]), str(files["inside"])}


def test_notes_rank_by_meaning_when_the_words_differ(notes, monkeypatch) -> None:
    manifest, files = notes

    def vector(text: str) -> list[float]:
        return [1.0, 0.0, 0.0] if "cache ahead" in text else [0.0, 1.0, 0.0]

    monkeypatch.setattr("max_chronicle.embeddings.embed_documents", lambda texts, **_: [vector(t) for t in texts])
    monkeypatch.setattr("max_chronicle.embeddings.embed_query", lambda query: [0.9, 0.1, 0.0])
    report = sync_notes(manifest)
    assert report["embedding"]["embedded"] == report["embedding"]["missing"] > 0

    hits = query_memory(manifest, query="what do we wipe prior to shipping")["notes"]

    assert hits[0]["path"] == str(files["global"]) and hits[0]["heading"] == "Release cache rule › Rule"
    assert hits[0]["channels"]["fts_rank"] is None and hits[0]["channels"]["vector_similarity"] > 0.9


def test_a_note_that_holds_every_term_makes_recall_confident(notes) -> None:
    manifest, _ = notes
    sync_notes(manifest)

    assert query_memory(manifest, query="orbit launches tuesdays")["no_confident_match"] is False
    without = query_memory(manifest, query="orbit launches tuesdays", include_notes=False)
    assert without["no_confident_match"] is True and "notes" not in without


def test_nothing_is_indexed_without_a_notes_section(loaded_manifest) -> None:
    report = sync_notes(loaded_manifest)
    assert report["enabled"] is False and report["indexed"] == 0
    assert query_memory(loaded_manifest, query="anything at all")["notes"] == []


def test_the_note_resource_serves_the_whole_redacted_note(notes, chronicle_sandbox) -> None:
    manifest, files = notes
    sync_notes(manifest)
    identifier = query_memory(manifest, query="identity deploy renders")["notes"][0]["document_id"]
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)

    contents = asyncio.run(server.read_resource(f"chronicle://note/{identifier}"))
    note = json.loads(contents[0].content)

    assert note["path"] == str(files["global"])
    assert note["text"] == ("## Release cache rule › Rule\n\nClear the build cache ahead of every release, always.\n\n"
                            "## Release cache rule › Why\n\nA stale cache broke the identity of the deploy pipeline renders.")


def test_the_golden_eval_scores_events_only(notes, monkeypatch) -> None:
    manifest, _ = notes
    sync_notes(manifest)
    event = service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": "Orbit launch moved"})

    def forbidden(*args, **kwargs):
        raise AssertionError("the golden eval must not rank notes")

    monkeypatch.setattr("max_chronicle.recall.recall_notes", forbidden)
    [detail] = run_eval(manifest, [GoldenCase(id="orbit", query="orbit launch moved", category="fact",
                                               expected=(f"event:{event['id']}",), lang="en")])
    assert "error" not in detail and detail["first_hit_rank"] == 1


def test_a_long_running_server_syncs_notes_on_its_own(notes, chronicle_sandbox) -> None:
    manifest, files = notes
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("\n[notes]\npaths = [" + ", ".join(json.dumps(p) for p in manifest["notes"]["paths"])
                     + "]\nsync_minutes = 60\n")
    toml_projects = '\n[[projects]]\nid = "orbit"\nroots = [' + json.dumps(manifest["projects"][0]["roots"][0]) + "]\n"
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write(toml_projects)

    thread = _start_notes_sync(chronicle_sandbox.manifest_path)

    assert thread is not None and thread.daemon
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and len(_documents(manifest)) < 4:
        time.sleep(0.05)
    assert str(files["orbit"]) in _documents(manifest)


def test_sections_follow_headings_outside_code_and_long_ones_split() -> None:
    body = ("# Title\nintro line\n## First\n```sh\n# not a heading\n```\n"
            "## Second\n" + ("word " * 200 + "\n\n") * 3)
    chunks = chunk_note("Title", body)

    assert [heading for heading, _ in chunks[:2]] == ["Title", "Title › First"]
    assert "# not a heading" in chunks[1][1]
    assert {heading for heading, _ in chunks[2:]} == {"Title › Second"} and len(chunks) > 3
    assert all(len(text) <= CHUNK_CHARS for _, text in chunks)


def test_a_note_belongs_to_a_root_a_project_folder_or_nobody(tmp_path) -> None:
    registry = Registry.from_manifest({"projects": [{"id": "max-chronicle", "aliases": ["status"]}]})
    workspace = tmp_path / "Projects"
    root = tmp_path / "code" / "orbit"
    manifest = {"paths": {"workspace_root": str(workspace)},
                "projects": [{"id": "orbit", "roots": [str(root)]}, {"id": "max-chronicle", "aliases": ["status"]}]}
    claude = tmp_path / ".claude" / "projects"

    assert note_project(root / "docs" / "a.md", manifest, registry) == "orbit"
    assert note_project(claude / _directory_slug(root) / "memory" / "a.md", manifest, registry) == "orbit"
    assert note_project(claude / (_directory_slug(workspace) + "-status") / "memory" / "a.md", manifest, registry) \
        == "max-chronicle"
    assert note_project(claude / _directory_slug(workspace) / "memory" / "a.md", manifest, registry) is None
    assert note_project(tmp_path / "elsewhere" / "a.md", manifest, registry) is None


# Review of #14: every test below failed before its fix.


def _pem_block(lines: int = 40) -> tuple[str, list[str]]:
    body = [base64.b64encode(secrets.token_bytes(48)).decode() for _ in range(lines)]
    head, tail = "-----BEGIN " + "RSA PRIVATE KEY-----", "-----END " + "RSA PRIVATE KEY-----"
    return "\n".join([head, *body, tail]), body


def _stored_text(manifest: dict) -> str:
    """Every string the note index stores, full-text content included."""
    with open_connection(config_from_manifest(manifest)) as connection:
        return json.dumps([[tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                           for table in ("documents", "document_chunks", "document_chunks_fts")])


def test_secrets_are_filtered_before_a_note_is_split(notes) -> None:
    """A private key longer than a section, or a key across a hard cut, never reaches the index."""
    manifest, files = notes
    block, body = _pem_block()
    key = _synthetic_key()
    long_line = "word " * 298 + "x " + key  # one 1,500+ character line, the key across the cut
    note = _note(files["global"].with_name("build-box.md"),
                 f"## Build box\nSSH access:\n\n{block}\n\n## Long\n{long_line}\n", name="Build box")

    sync_notes(manifest)

    stored = _stored_text(manifest)
    assert not any(line in stored for line in body)
    assert key not in "".join(text for _, text in _chunks(manifest, note))


def test_a_key_in_a_file_name_or_type_is_never_stored(notes) -> None:
    manifest, files = notes
    token = "ghp_" + secrets.token_hex(18)
    key = _synthetic_key()
    named = _note(files["global"].with_name(f"{token}.md"), "## Setup\nPlain setup text.\n", name="Setup note")
    typed = _note(files["global"].with_name("vendor-setup.md"), "## Setup\nThe vendor sandbox rotates weekly.\n",
                  name="Vendor setup", type=key)
    ssh = _note(files["global"].with_name("build-box-private-key.md"), "## Box\nPlain text.\n", name="Box")

    report = sync_notes(manifest)

    documents = _documents(manifest)
    assert str(named) not in documents and str(ssh) not in documents and str(typed) in documents
    assert token not in _stored_text(manifest) and key not in _stored_text(manifest)
    assert token not in json.dumps(report) and report["denied"] == 1
    assert [item["reason"] for item in report["skipped"]] == ["secret_in_path"]
    found = query_memory(manifest, query="vendor sandbox rotates")["notes"]
    assert found and key not in json.dumps(found) and key not in json.dumps(read_note(manifest, found[0]["document_id"]))


def test_full_text_matches_are_scoped_before_they_are_capped(notes) -> None:
    manifest, files = notes
    for index in range(120):
        _note(files["atlas"].parent / f"step-{index:03d}.md", f"## Deploy {index}\nDeploy pipeline step {index}.\n",
              name=f"Atlas step {index}")
    filler = " ".join(f"word{index}" for index in range(160))
    _note(files["inside"], f"# Orbit notes\n\nThe orbit deploy pipeline has no staging. {filler}\n")
    sync_notes(manifest)

    found = query_memory(manifest, query="deploy pipeline", project="orbit")

    assert [note["path"] for note in found["notes"]][:2] == [str(files["inside"]), str(files["global"])]
    assert found["no_confident_match"] is False


def _orbit_vector(manifest: dict, files: dict) -> list[float] | None:
    with open_connection(config_from_manifest(manifest)) as connection:
        row = connection.execute(
            "SELECT v.vector FROM document_chunks c JOIN documents d ON d.id = c.document_id "
            "LEFT JOIN chunk_vectors v ON v.chunk_id = c.id WHERE d.path = ?", (str(files["orbit"]),)).fetchone()
    return unpack_vector(row["vector"]) if row["vector"] else None


def test_a_vector_never_outlives_the_text_it_was_made_from(notes, monkeypatch) -> None:
    manifest, files = notes
    batches: list[int] = []

    def embed(texts, **_):
        if not batches:  # another sync re-indexes a changed note while this batch is embedded
            _note(files["orbit"], "## Launch\nOrbit launches on Fridays now.\n", name="Orbit launch")
            sync_notes(manifest, embed=False)
        batches.append(len(texts))
        return [[1.0, 0.0, 0.0] if "Tuesdays" in text else [0.0, 1.0, 0.0] for text in texts]

    monkeypatch.setattr("max_chronicle.embeddings.embed_documents", embed)
    sync_notes(manifest)
    assert _orbit_vector(manifest, files) is None  # the vector of the old text was not stored

    again = sync_notes(manifest)

    assert again["embedding"]["embedded"] == 1 and _orbit_vector(manifest, files) == [0.0, 1.0, 0.0]


def test_symlinks_are_not_followed(notes, tmp_path) -> None:
    manifest, files = notes
    outside = tmp_path / "outside"
    _note(outside / "vendor-api-keys.md", "## Keys\nzebramarker lives in a denied file\n")
    _note(outside / "project" / "memory" / "plan.md", "## Plan\nquokkamarker lives outside the paths\n")
    (files["global"].parent / "harmless.md").symlink_to(outside / "vendor-api-keys.md")
    (files["atlas"].parents[2] / "linked-project").symlink_to(outside / "project", target_is_directory=True)

    report = sync_notes(manifest)

    stored = _stored_text(manifest)
    assert "zebramarker" not in stored and "quokkamarker" not in stored
    assert [item["reason"] for item in report["skipped"]] == ["symlink", "symlink"]


def test_notes_commands_use_the_db_flag(notes, chronicle_sandbox, monkeypatch, capsys, tmp_path) -> None:
    manifest, _ = notes
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("\n[notes]\npaths = [" + ", ".join(json.dumps(p) for p in manifest["notes"]["paths"])
                     + ']\nexclude = ["*.bak-*"]\n')
    other = tmp_path / "copy" / "chronicle.db"
    other.parent.mkdir()
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "--db", str(other), "notes", "sync", "--no-embed"])

    assert cli.main() == 0
    capsys.readouterr()

    assert _documents(manifest) == {}
    with open_connection(replace(config_from_manifest(manifest), db_path=other)) as connection:
        assert connection.execute("SELECT count(*) FROM documents").fetchone()[0] == 4


def test_the_startup_bundle_takes_no_confidence_from_notes(notes) -> None:
    manifest, _ = notes
    sync_notes(manifest)
    service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": "Grocery list for the weekend"})

    bundle = service.build_startup_bundle(manifest, focus="orbit launches tuesdays")

    assert bundle["recall_status"]["no_confident_match"] is True


def test_a_nested_memory_folder_belongs_to_the_enclosing_project(tmp_path) -> None:
    workspace = tmp_path / "Projects"
    orbit = workspace / "orbit"
    manifest = {"paths": {"workspace_root": str(workspace)}, "projects": [{"id": "orbit", "roots": [str(orbit)]}]}
    registry = Registry.from_manifest(manifest)
    for folder in (orbit / ".worktrees" / "feature", orbit / "docs", workspace / "voice" / "transcripts",
                   workspace / "orbit-legacy", workspace / "garden planner" / "notes"):
        folder.mkdir(parents=True)

    def project(folder: Path) -> str | None:
        return note_project(tmp_path / ".claude" / "projects" / _directory_slug(folder) / "memory" / "a.md",
                            manifest, registry)

    assert project(orbit / ".worktrees" / "feature") == "orbit"
    assert project(orbit / "docs") == "orbit"
    assert project(workspace / "voice" / "transcripts") == "voice"
    assert project(workspace / "orbit-legacy") == "orbit-legacy"
    assert project(workspace / "garden planner" / "notes") == "garden-planner"
    # The directory is gone: a hidden folder under a root stays with the root, a workspace child keeps its name.
    assert project(orbit / ".worktrees" / "removed") == "orbit"
    assert project(workspace / "archived-app") == "archived-app"
    assert project(workspace / "archived-app" / ".cache") == "archived-app"
    (workspace / "orbit-legacy").rmdir()
    assert project(workspace / "orbit-legacy") == "orbit-legacy"


def test_only_utf8_text_is_read(notes) -> None:
    """A byte-order mark can lie about the bytes after it, so a note must be UTF-8 (a UTF-8 mark is fine)."""
    manifest, files = notes
    key = _synthetic_key()
    line = f"OPENAI_API_KEY={key}\n"
    utf16 = _note_bytes(files, "vendor-utf16.md", f"## Vendor\nThe vendor sandbox rotates weekly. {line}".encode("utf-16"))
    mixed = _note_bytes(files, "vendor-mixed.md", codecs.BOM_UTF16_LE + (line + " " * (len(line) % 2)).encode("ascii"))
    appended = _note_bytes(files, "vendor-appended.md",
                           "## Vendor\nThe vendor sandbox rotates weekly.\n".encode("utf-16") + line.encode("ascii"))
    binary = _note_bytes(files, "blob.md", b"## Blob\n\x00\x01zebramarker\x00")
    latin = _note_bytes(files, "latin.md", "## Caf\xe9\nquokkamarker menu\n".encode("latin-1"))
    marked = _note_bytes(files, "marked.md", codecs.BOM_UTF8 + "## Marked\nThe marked note reads fine.\n".encode())

    report = sync_notes(manifest)

    raw = _raw_strings(manifest)
    wide = raw.encode("utf-16-le", errors="ignore")  # a key read as UTF-16 comes back when encoded again
    assert key not in raw and key.encode() not in wide and key.encode() not in wide[1:]
    assert "zebramarker" not in raw and "quokkamarker" not in raw
    skipped = {item["path"] for item in report["skipped"] if item["reason"] == "not_text"}
    assert skipped == {str(path) for path in (utf16, mixed, appended, binary, latin)}
    assert _chunks(manifest, marked) == [("marked › Marked", "The marked note reads fine.")]


def _raw_strings(manifest: dict) -> str:
    with open_connection(config_from_manifest(manifest)) as connection:
        return "\n".join(value for table in ("documents", "document_chunks", "document_chunks_fts")
                         for row in connection.execute(f"SELECT * FROM {table}") for value in row
                         if isinstance(value, str))


def _note_bytes(files: dict, name: str, data: bytes) -> Path:
    path = files["global"].with_name(name)
    path.write_bytes(data)
    return path


@pytest.mark.parametrize("size", [13, 16])
def test_a_malformed_note_vector_is_ignored(notes, monkeypatch, size) -> None:
    manifest, _ = notes
    sync_notes(manifest, embed=False)
    service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": "Orbit launch moved"})
    monkeypatch.setattr("max_chronicle.embeddings.embed_query", lambda query: [0.9, 0.1, 0.0])
    with open_connection(config_from_manifest(manifest)) as connection:
        chunk_id = connection.execute("SELECT id FROM document_chunks ORDER BY id LIMIT 1").fetchone()[0]
        connection.execute("INSERT INTO chunk_vectors(chunk_id, model_key, dim, vector, created_at_utc) "
                           "VALUES (?, ?, 3, ?, '2026-09-24T00:00:00Z')", (chunk_id, active_profile().key, b"\x01" * size))
        connection.commit()

    payload = query_memory(manifest, query="orbit launch moved")

    assert payload["results"] and payload["notes"] and "notes" not in payload["channel_errors"]


def test_a_relaxed_note_match_is_flagged(notes) -> None:
    manifest, _ = notes
    sync_notes(manifest)

    payload = query_memory(manifest, query="orbit launching tuesday")

    assert payload["notes"] and payload["relaxed"] is True and "notes_fts" in payload["channels_used"]


def test_a_bad_notes_section_stops_the_server_cleanly(chronicle_sandbox, monkeypatch, caplog) -> None:
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write('\n[notes]\npaths = ["/nonexistent/*.md"]\nsync_minutes = 0\n')
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "--transport", "streamable-http"])

    with caplog.at_level(logging.ERROR, logger="max_chronicle.mcp"):
        assert mcp_server_module._main() == 1

    assert "cannot start" in caplog.text and "sync_minutes" in caplog.text


def test_note_paths_must_be_absolute() -> None:
    with pytest.raises(ValueError, match="absolute"):
        note_settings({"notes": {"paths": ["notes/*.md"]}})
    assert note_settings({"notes": {"paths": ["~/notes/*.md"]}}).enabled


def test_titles_and_frontmatter_ignore_code_and_rules(notes) -> None:
    manifest, files = notes
    memory = files["global"].parent
    code = _note(memory / "toolchain.md", "```sh\n# install the toolchain\nmake setup\n```\n\n## Usage\nRun it once.\n")
    ruled = _note(memory / "ruled.md", "---\nThe opening paragraph after a rule.\n\n---\n\n## Later\nMore text.\n")

    sync_notes(manifest)

    assert _documents(manifest)[str(code)]["title"] == "toolchain"
    assert _chunks(manifest, code)[0] == ("toolchain", "```sh\n# install the toolchain\nmake setup\n```")
    assert "The opening paragraph after a rule." in _chunks(manifest, ruled)[0][1]


def test_equally_ranked_notes_come_newest_first(notes, monkeypatch) -> None:
    manifest, files = notes
    memory = files["global"].parent
    older = _note(memory / "older.md", "## Walrus\nwalrus harbor schedule\n", name="Older walrus")
    newer = _note(memory / "newer.md", "## Otter\nsomething unrelated entirely\n", name="Newer otter")
    os.utime(older, (1_600_000_000, 1_600_000_000))
    os.utime(newer, (1_700_000_000, 1_700_000_000))
    monkeypatch.setattr("max_chronicle.embeddings.embed_documents",
                        lambda texts, **_: [[1.0, 0.0, 0.0] if "unrelated" in t else [0.0, 1.0, 0.0] for t in texts])
    monkeypatch.setattr("max_chronicle.embeddings.embed_query", lambda query: [1.0, 0.0, 0.0])
    sync_notes(manifest)

    hits = query_memory(manifest, query="walrus harbor")["notes"]

    assert hits[0]["rrf_score"] == hits[1]["rrf_score"]
    assert [hit["path"] for hit in hits[:2]] == [str(newer), str(older)]


def test_a_note_denied_later_leaves_nothing_behind(notes) -> None:
    manifest, files = notes
    note = _note(files["global"].with_name("plans.md"), "## Plans\nquokkamarker zebramarker\n",
                 name="Plans quokkamarker", description="zebramarker plans", type="walrusmarker")
    sync_notes(manifest)
    identifier = _documents(manifest)[str(note)]["id"]

    assert sync_notes({**manifest, "notes": {**manifest["notes"], "deny": ["plans.md"]}})["removed"] == 1

    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        row = dict(connection.execute("SELECT * FROM documents WHERE id = ?", (identifier,)).fetchone())
        segments = b"".join(bytes(block) for (block,) in connection.execute(
            "SELECT block FROM document_chunks_fts_data") if block is not None)
    assert row["deleted_at_utc"] is not None
    assert not {"quokkamarker", "zebramarker", "walrusmarker"} & set(json.dumps(row).replace('"', " ").split())
    assert b"quokkamarker" not in segments and read_note(manifest, identifier) is None
    raw = b"".join(path.read_bytes() for path in config.db_path.parent.glob(config.db_path.name + "*"))
    assert b"quokkamarker" not in raw and b"zebramarker" not in raw


# Second review of #14: every test below failed before its fix, except the guards marked as such.

_OPENSSH_BEGIN = "-----BEGIN " + "OPENSSH PRIVATE" + " KEY-----"
_OPENSSH_END = "-----END " + "OPENSSH PRIVATE" + " KEY-----"


def test_a_key_pattern_never_crosses_a_heading(notes) -> None:
    manifest, files = notes
    memory = files["global"].parent
    cut = _note(memory / "deploy-key-howto.md", "# Deploy key howto\n\n## Format\nThe key file begins with this line:\n"
                + _OPENSSH_BEGIN + "\n\n## Rotation\nRotate the deploy key every month.\n")
    mentions = _note(memory / "farm-key-notes.md",
                     "# Farm key notes\n\n## Header\nA key file starts with the line " + _OPENSSH_BEGIN + " on its own.\n\n"
                     "## Where it lives\nThe render farm deploy key lives in the password manager entry farm.\n\n"
                     "## Footer\nIt ends with the line " + _OPENSSH_END + " at the bottom.\n")
    body = [base64.b64encode(secrets.token_bytes(52)).decode()[:70] for _ in range(6)]
    whole = _note(memory / "farm-access.md", "# Farm access\n\n## Key\n" + _OPENSSH_BEGIN + "\n" + "\n".join(body)
                  + "\n" + _OPENSSH_END + "\n\n## After\nUse the bastion host.\n")

    sync_notes(manifest)

    assert [heading for heading, _ in _chunks(manifest, cut)] == ["Deploy key howto › Format", "Deploy key howto › Rotation"]
    assert "password manager entry farm" in " ".join(text for _, text in _chunks(manifest, mentions))
    stored = _raw_strings(manifest)
    assert not any(line in stored for line in body)  # guard: a whole block still goes whole
    assert [heading for heading, _ in _chunks(manifest, whole)] == ["Farm access › Key", "Farm access › After"]


class _PoolThenSync:
    """A connection that lets a sync commit right after recall reads its pool of chunks."""

    def __init__(self, connection, sync):
        self._connection, self._sync = connection, sync

    def execute(self, sql, *args):
        cursor = self._connection.execute(sql, *args)
        if self._sync and "LEFT JOIN chunk_vectors v" in sql:
            rows = cursor.fetchall()
            self._sync.pop()()
            return rows
        return cursor

    def __getattr__(self, name):
        return getattr(self._connection, name)


def test_recall_survives_a_sync_between_its_reads(notes, monkeypatch) -> None:
    manifest, files = notes
    sync_notes(manifest)
    real = notes_module.open_connection

    def concurrent_sync():
        _note(files["global"].with_name("kraken.md"), "## Kraken\nThe kraken release checklist.\n", name="Kraken")
        monkeypatch.setattr(notes_module, "open_connection", real)
        sync_notes(manifest)

    hooks = [concurrent_sync]

    @contextlib.contextmanager
    def racing(config):
        with real(config) as connection:
            yield _PoolThenSync(connection, hooks)

    monkeypatch.setattr(notes_module, "open_connection", racing)

    found = query_memory(manifest, query="kraken release checklist")

    assert not hooks and "notes" not in found["channel_errors"]
    assert query_memory(manifest, query="kraken release checklist")["notes"][0]["title"] == "Kraken"


def test_global_notes_do_not_crowd_out_the_project_note(notes) -> None:
    manifest, files = notes
    filler = " ".join(f"filler{index}" for index in range(300))
    project_note = _note(files["inside"].parent / "quasar-plan.md", f"## Plan\nThe quasar survey. {filler}\n")
    for index in range(101):
        _note(files["global"].parent / f"quasar-{index:03d}.md", f"## Q{index}\nquasar quasar\n")
    sync_notes(manifest, embed=False)

    found = query_memory(manifest, query="quasar", project="orbit")["notes"]

    assert found[0]["path"] == str(project_note)


def test_an_unreadable_directory_entry_does_not_stop_a_sync(notes, tmp_path) -> None:
    manifest, files = notes
    base = tmp_path / "wk"
    base.mkdir()
    (base / "loop").symlink_to("loop")  # stat() fails with ELOOP
    claude_projects = files["global"].parents[2]  # <projects>/<folder>/memory/<note>
    _note(claude_projects / _directory_slug(base / "loop" / "sub") / "memory" / "plan.md", "## Plan\nquokka plan\n")

    assert _slug_directory(_directory_slug(base / "loop" / "sub")) is None
    assert sync_notes(manifest, embed=False)["indexed"] == 5


def test_recursive_patterns_do_not_follow_directory_symlinks(tmp_path) -> None:
    folder = tmp_path / "notes"
    _note(folder / "a.md", "## A\nx\n")
    _note(folder / ".trash" / "old.md", "## Old\nx\n")
    (folder / "loop").symlink_to(".", target_is_directory=True)

    plain, linked = discover(note_settings({"notes": {"paths": [str(folder / "**" / "*.md")]}}))

    assert (plain, linked) == ([folder / "a.md"], [])


FRONTMATTERS = {
    "spaces_in_key": "---\nname: Deploy checklist\ndate created: 2026-09-01\ntype: reference\n---\n",
    "quoted_key": "---\n\"name\": Deploy checklist\ntype: reference\n---\n",
    "cyrillic_key": "---\nname: Deploy checklist\nавтор: Орбита\ntype: reference\n---\n",
    "slash_in_key": "---\nname: Deploy checklist\nog/image: cover.png\ntype: reference\n---\n",
    "nested_and_folded": ("---\nname: Deploy checklist\ntype: reference\ndescription: >\n  A folded\n  description\n"
                          "metadata:\n  owner: orbit\n  tags:\n    - a\n    - b\n---\n"),
}


def test_frontmatter_takes_any_key_and_needs_one(notes) -> None:
    manifest, files = notes
    memory = files["global"].parent
    paths = {tag: _note(memory / f"fm-{tag.replace('_', '-')}.md", head + "## Steps\nRun the checklist twice.\n")
             for tag, head in FRONTMATTERS.items()}
    procedure = _note(memory / "procedure.md",
                      "---\n# Deploy procedure\n- Stop the worker first\n- Run the migration after\n---\n\n## Notes\nMore.\n")

    sync_notes(manifest, embed=False)

    documents = _documents(manifest)
    assert {tag: (documents[str(path)]["title"], documents[str(path)]["kind"]) for tag, path in paths.items()} == {
        tag: ("Deploy checklist", "reference") for tag in FRONTMATTERS}
    assert "Stop the worker first" in " ".join(text for _, text in _chunks(manifest, procedure))


def test_code_fences_close_only_on_their_own_marker(notes) -> None:
    manifest, files = notes
    memory = files["global"].parent
    nested = _note(memory / "snippets.md", "````markdown\n```sh\nmake setup\n```\n# Fake title inside a snippet\n````\n\n"
                   "# Actual title\n\n## Use\nRun it.\n")
    mixed = _note(memory / "manual.md", "~~~\n```\n# Fake title inside a tilde fence\n~~~\n\n# Actual manual\n\nSteps.\n")

    sync_notes(manifest, embed=False)

    documents = _documents(manifest)
    assert documents[str(nested)]["title"] == "Actual title" and documents[str(mixed)]["title"] == "Actual manual"
    assert [heading for heading, _ in _chunks(manifest, nested)] == ["Actual title", "Actual title › Use"]


def test_equal_similarity_ranks_the_newer_note_first(notes, monkeypatch) -> None:
    manifest, files = notes
    memory = files["global"].parent
    older = _note(memory / "a-old.md", "## Walrus\nsame walrus harbor text\n", name="Old walrus")
    newer = _note(memory / "b-new.md", "## Walrus\nsame walrus harbor text\n", name="New walrus")
    os.utime(older, (1_600_000_000, 1_600_000_000))
    os.utime(newer, (1_700_000_000, 1_700_000_000))
    monkeypatch.setattr("max_chronicle.embeddings.embed_documents",
                        lambda texts, **_: [[1.0, 0.0, 0.0] if "walrus" in t else [0.0, 1.0, 0.0] for t in texts])
    monkeypatch.setattr("max_chronicle.embeddings.embed_query", lambda query: [1.0, 0.0, 0.0])
    sync_notes(manifest)

    hits = [hit for hit in query_memory(manifest, query="pinniped")["notes"] if "walrus" in hit["title"].lower()]

    assert [hit["path"] for hit in hits] == [str(newer), str(older)]  # the tie breaks in the channel's own ranks


@pytest.mark.parametrize("line", ['notes = "on"', 'notes = ["~/notes/*.md"]'])
def test_a_notes_value_that_is_not_a_table_stops_the_server_cleanly(chronicle_sandbox, monkeypatch, caplog, line) -> None:
    text = chronicle_sandbox.manifest_path.read_text(encoding="utf-8")
    chronicle_sandbox.manifest_path.write_text(line + "\n" + text, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "--transport", "streamable-http"])

    with caplog.at_level(logging.ERROR, logger="max_chronicle.mcp"):
        assert mcp_server_module._main() == 1

    assert "cannot start" in caplog.text and "[notes] must be a table" in caplog.text


@pytest.mark.parametrize("argv", [["--transport", "stdio"], ["--profile", "readonly", "--transport", "streamable-http"]])
def test_a_server_that_never_syncs_ignores_a_bad_notes_section(chronicle_sandbox, loaded_manifest, monkeypatch,
                                                              argv) -> None:
    with open_connection(config_from_manifest(loaded_manifest)):
        pass  # a read-only server needs a database to exist
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write('\n[notes]\npaths = ["/nonexistent/*.md"]\nsync_minutes = 0\n')
    monkeypatch.setattr(mcp_server_module, "build_server",
                        lambda *args, **kwargs: SimpleNamespace(run=lambda transport: None, settings=SimpleNamespace()))
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path), *argv])

    assert mcp_server_module._main() == 0


def test_notes_are_indexed_again_when_the_index_changes(notes, monkeypatch) -> None:
    manifest, _ = notes
    sync_notes(manifest)
    assert sync_notes(manifest)["unchanged"] == 4

    monkeypatch.setattr(notes_module, "NOTE_INDEX_VERSION", NOTE_INDEX_VERSION + 1)

    assert (sync_notes(manifest)["indexed"], sync_notes(manifest)["unchanged"]) == (4, 4)


@pytest.mark.parametrize("command", [["notes", "status"], ["notes", "sync", "--no-embed"], ["recent"]])
def test_the_db_flag_works_without_a_paths_table(tmp_path, monkeypatch, capsys, command) -> None:
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(f'version = 1\n[notes]\npaths = ["{tmp_path}/notes/*.md"]\n', encoding="utf-8")
    monkeypatch.setenv("CHRONICLE_ROOT", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(manifest), "--db", str(tmp_path / "x.db"), *command])

    assert cli.main() == 0
    capsys.readouterr()


def test_the_folder_walk_prefers_the_shallower_directory_and_stays_in_budget(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "Projects"
    (workspace / "a" / "b").mkdir(parents=True)
    (workspace / "a-b").mkdir()
    assert _slug_directory(_directory_slug(workspace / "a-b")) == workspace / "a-b"

    wide = tmp_path / "wide"
    wide.mkdir()
    for index in range(200):
        (wide / f"f{index:03d}").touch()
    read = []
    real = os.scandir

    @contextlib.contextmanager
    def counting(path):
        with real(path) as listing:
            yield (read.append(entry.name) or entry for entry in listing)

    monkeypatch.setattr(notes_module, "WALK_BUDGET", 50)
    monkeypatch.setattr(notes_module.os, "scandir", counting)

    # A listing is read to its end or one entry past the budget, which tells the two apart.
    assert _slug_directory(_directory_slug(wide / "missing" / "sub")) is None and len(read) <= 51


def test_browse_search_shows_notes(notes, capsys) -> None:
    manifest, _ = notes
    sync_notes(manifest)
    payload = query_memory(manifest, query="orbit launches tuesdays")
    capsys.readouterr()

    render_search_results(payload)

    assert "Orbit launch" in capsys.readouterr().out


def test_removed_text_leaves_the_write_ahead_log_too(notes) -> None:
    manifest, files = notes
    sync_notes(manifest, embed=False)
    db = config_from_manifest(manifest).db_path
    idle = sqlite3.connect(db)  # another process's idle connection keeps the log file alive
    try:
        idle.execute("SELECT 1").fetchone()
        note = _note(files["global"].with_name("zanzibar.md"), "## Z\nThe zanzibarquokka plan.\n", name="Zanzibar")
        sync_notes(manifest, embed=False)
        sync_notes({**manifest, "notes": {**manifest["notes"], "deny": ["zanzibar*"]}}, embed=False)

        assert read_note(manifest, document_id(note)) is None
        for path in (db, Path(f"{db}-wal")):
            assert not path.exists() or b"zanzibarquokka" not in path.read_bytes()
    finally:
        idle.close()


def test_a_parent_step_after_a_symlink_follows_the_filesystem(tmp_path) -> None:
    (tmp_path / "x" / "y").mkdir(parents=True)
    _note(tmp_path / "x" / "notes" / "physical.md", "# p\n")
    _note(tmp_path / "notes" / "lexical.md", "# l\n")
    (tmp_path / "link").symlink_to(tmp_path / "x" / "y", target_is_directory=True)

    plain, linked = discover(note_settings({"notes": {"paths": [str(tmp_path / "link" / ".." / "notes" / "*.md")]}}))

    assert [path.name for path in plain] == ["physical.md"] and linked == []


# Third review of #14: every test below failed before its fix, except the guards marked as such.


def test_a_key_block_split_by_a_heading_is_still_removed(notes) -> None:
    manifest, files = notes
    memory = files["global"].parent
    body = [base64.b64encode(secrets.token_bytes(48)).decode() for _ in range(8)]
    _note(memory / "heading-begin.md", "# Box\n\n## " + _OPENSSH_BEGIN + "\n" + "\n".join(body[:4]) + "\n" + _OPENSSH_END
          + "\n\n## After\nUse the bastion.\n")
    _note(memory / "interrupted.md", "# Box two\n\n" + _OPENSSH_BEGIN + "\n" + "\n".join(body[4:6])
          + "\n## Pasted in the middle\n" + "\n".join(body[6:]) + "\n" + _OPENSSH_END + "\n")

    sync_notes(manifest, embed=False)

    stored = _raw_strings(manifest)
    assert not any(line[8:40] in stored for line in body)


def test_frontmatter_parsing_stays_linear(notes) -> None:
    manifest, files = notes
    note = _note(files["global"].with_name("spaces.md"), "---\na" + " " * 60_000 + "\n---\n\n## Body\nText.\n")

    started = time.perf_counter()
    sync_notes(manifest, embed=False)

    assert time.perf_counter() - started < 1.0 and "Text." in _chunks(manifest, note)[-1][1]


def test_a_block_scalar_does_not_lend_its_lines_to_the_metadata(notes) -> None:
    manifest, files = notes
    note = _note(files["global"].with_name("scalar.md"), "---\ndescription: |\n  name: an example in prose\n"
                 "  type: some text\nname: Real title\ntype: reference\n---\n\n## Body\nText.\n")

    sync_notes(manifest, embed=False)

    row = _documents(manifest)[str(note)]
    assert (row["title"], row["kind"], row["description"]) == ("Real title", "reference", None)


class _FakeEntry:
    def __init__(self, parent: str, name: str):
        self.name, self.path = name, f"{parent.rstrip('/')}/{name}"

    def is_dir(self) -> bool:
        return True


def test_a_folder_found_with_the_last_of_the_budget_counts(monkeypatch) -> None:
    tree = {"/": ["project"], "/project": ["target"], "/project/target": []}

    @contextlib.contextmanager
    def listing(path):
        yield iter(_FakeEntry(str(path), name) for name in tree[str(path)])

    monkeypatch.setattr(notes_module, "WALK_BUDGET", 2)
    monkeypatch.setattr(notes_module.os, "scandir", listing)

    assert _slug_directory("-project-target") == Path("/project/target")


def test_a_checkpoint_blocked_by_a_reader_is_retried_by_the_next_sync(notes) -> None:
    manifest, files = notes
    sync_notes(manifest, embed=False)
    db = config_from_manifest(manifest).db_path
    reader = sqlite3.connect(db)
    try:
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM documents").fetchone()  # holds a snapshot: the log cannot be emptied
        note = _note(files["global"].with_name("walrus.md"), "## W\nThe walrusmarker plan.\n", name="Walrus")
        sync_notes(manifest, embed=False)
        sync_notes({**manifest, "notes": {**manifest["notes"], "deny": ["walrus*"]}}, embed=False)
        reader.commit()

        sync_notes({**manifest, "notes": {**manifest["notes"], "deny": ["walrus*"]}}, embed=False)  # nothing changes

        assert read_note(manifest, document_id(note)) is None
        for path in (db, Path(f"{db}-wal")):
            assert not path.exists() or b"walrusmarker" not in path.read_bytes()
    finally:
        reader.close()


class _PoolThenReindex(_PoolThenSync):
    """Re-index a note under the same chunk ids right after recall reads its pool."""


def test_recall_reads_its_pool_and_matches_from_one_snapshot(notes, monkeypatch) -> None:
    manifest, files = notes
    sync_notes(manifest)
    real = notes_module.open_connection

    def reindex():
        _note(files["orbit"], "## Launch\nNew quasar deployment instructions.\n", name="Orbit launch")
        monkeypatch.setattr(notes_module, "open_connection", real)
        sync_notes(manifest, embed=False)

    hooks = [reindex]

    @contextlib.contextmanager
    def racing(config):
        with real(config) as connection:
            yield _PoolThenReindex(connection, hooks)

    monkeypatch.setattr(notes_module, "open_connection", racing)

    found = query_memory(manifest, query="quasar deployment")

    assert not hooks and all("quasar" in note["text"].lower() for note in found["notes"])


def test_a_database_from_the_first_note_index_gains_the_version_column(chronicle_sandbox, loaded_manifest,
                                                                        tmp_path) -> None:
    from max_chronicle.config import MIGRATIONS_DIR
    from max_chronicle.store import prepare_database

    early = tmp_path / "migrations"
    early.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= 13:
            (early / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    config = config_from_manifest(loaded_manifest)
    with open_connection(replace(config, migrations_dir=early)):
        pass  # a database at schema 13, as the first note index built it

    prepare_database(config, allow_upgrade=True)
    with open_connection(config) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(documents)")}

    assert "index_version" in columns


def test_record_with_the_db_flag_and_no_paths_table_does_not_fail_after_writing(tmp_path, monkeypatch, capsys) -> None:
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("version = 1\n", encoding="utf-8")
    monkeypatch.setenv("CHRONICLE_ROOT", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(manifest), "--db", str(tmp_path / "x.db"),
                                      "record", "Written once", "--agent", "a", "--domain", "global"])

    assert cli.main() == 0
    capsys.readouterr()



# Fourth review of #14.


def test_a_note_with_lone_carriage_returns_keeps_no_key(notes) -> None:
    manifest, files = notes
    block, body = _pem_block(8)
    path = files["global"].with_name("old-mac.md")
    path.write_bytes(("# Old Mac note\r\r## Box\rAccess:\r" + block.split("\n-----END")[0] + "\r").replace("\n", "\r")
                     .encode())

    sync_notes(manifest)

    stored = _stored_text(manifest)
    assert not any(line[:40] in stored for line in body)
    assert [heading for heading, _ in _chunks(manifest, path)] == ["Old Mac note › Box"]


def test_a_quoted_frontmatter_key_may_hold_a_colon(notes) -> None:
    manifest, files = notes
    path = files["global"].with_name("quoted.md")
    path.write_text('---\n"external:id": 1\nname: Real title\n---\n# Body\nText.\n', encoding="utf-8")

    sync_notes(manifest)

    assert _documents(manifest)[str(path)]["title"] == "Real title"
    assert "external" not in "".join(text for _, text in _chunks(manifest, path))


@pytest.mark.parametrize("with_column", [False, True], ids=["0013-as-released", "0013-pre-release"])
def test_both_builds_of_0013_upgrade(chronicle_sandbox, loaded_manifest, tmp_path, with_column) -> None:
    from max_chronicle.config import MIGRATIONS_DIR
    from max_chronicle.store import prepare_database

    early = tmp_path / "migrations"
    early.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= 13:
            sql = path.read_text(encoding="utf-8")
            if with_column and path.name.startswith("0013"):
                sql = sql.replace("redactions      INTEGER NOT NULL DEFAULT 0\n",
                                  "redactions      INTEGER NOT NULL DEFAULT 0,\n    index_version   INTEGER NOT NULL DEFAULT 0\n")
                assert "index_version" in sql
            (early / path.name).write_text(sql, encoding="utf-8")
    config = config_from_manifest(loaded_manifest)
    with open_connection(replace(config, migrations_dir=early)):
        pass

    prepare_database(config, allow_upgrade=True)

    with open_connection(config) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(documents)")]
    assert columns.count("index_version") == 1


# The parser and the secret filter, as indexed notes were made with them.
# Changing either changes this digest: bump NOTE_INDEX_VERSION so every note
# is indexed again, then record the new digest under the new version.
INDEX_DIGESTS = {1: "f9eeb8b601a0e073ed95266fd734645932df286b7166377725d692a2549ba463"}


def test_the_index_version_moves_with_the_parser_and_the_filter() -> None:
    import hashlib
    import inspect

    from max_chronicle import redaction

    parts = [inspect.getsource(redaction)] + [inspect.getsource(getattr(notes_module, name)) for name in (
        "_frontmatter", "_key_value", "_is_key_line", "_split", "_in_code", "chunk_note", "_decode", "_first_title", "parse_note")]
    parts += [repr(getattr(notes_module, name)) for name in ("CHUNK_CHARS", "_HEADING", "_FENCE")]
    parts.append(repr(sorted(notes_module._BLOCK_SCALARS)))  # a set's order changes from run to run
    digest = hashlib.sha256("\n".join(parts).encode()).hexdigest()

    assert INDEX_DIGESTS.get(NOTE_INDEX_VERSION) == digest, (
        f"the parser or the filter changed: bump NOTE_INDEX_VERSION and record {digest}")
