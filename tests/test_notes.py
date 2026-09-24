"""The note index (FR-10, AC-14): notes written on purpose, recalled by every agent, secrets never stored."""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from pathlib import Path

import pytest

from max_chronicle.evals import GoldenCase, run_eval
from max_chronicle.mcp_server import _start_notes_sync, build_server
from max_chronicle.notes import CHUNK_CHARS, _directory_slug, chunk_note, note_project, read_note, sync_notes
from max_chronicle.identity import Registry
from max_chronicle.recall import query_memory
from max_chronicle.store import config_from_manifest, open_connection

from max_chronicle import service


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
