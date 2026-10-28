"""Ollama on demand: nothing calls it without work, a search or a sweep starts it, the model unloads."""

from __future__ import annotations

import io
import json
import threading
import urllib.error

import pytest

from max_chronicle import doctor, embeddings, mcp_server, service
from max_chronicle.embeddings import (
    BackendState,
    EmbedderSettings,
    embedder_settings,
    ensure_backend,
)
from max_chronicle.store import (
    config_from_manifest,
    fetch_event_ids_without_embedding,
    store_event_embedding,
)

# conftest stubs these for every test; keep the real ones.
_REAL_PROBE = embeddings.probe_backend
_REAL_EMBED_TEXT = embeddings.embed_text
START = EmbedderSettings(start_command=("open", "-g", "-a", "Ollama"))
DOWN = BackendState(up=False)
LOADED = BackendState(up=True, loaded=True, expires_at="2026-10-28T20:06:24+01:00")
UNLOADED = BackendState(up=True, loaded=False)
NOBODY = "http://127.0.0.1:9"  # discard port: nothing answers


class _Clock:
    """A monotonic clock that only the fake sleep moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _probe_sequence(monkeypatch, states: list[BackendState]) -> list[int]:
    """Ollama answers with *states* in turn and keeps the last one."""
    calls: list[int] = []

    def probe(*args, **kwargs):
        calls.append(1)
        return states.pop(0) if len(states) > 1 else states[0]

    monkeypatch.setattr(embeddings, "probe_backend", probe)
    return calls


def _launches(monkeypatch, *, fail: Exception | None = None) -> list[tuple[str, ...]]:
    launched: list[tuple[str, ...]] = []

    def launch(command):
        launched.append(command)
        if fail is not None:
            raise fail

    monkeypatch.setattr(embeddings, "_launch", launch)
    return launched


# ---------------------------------------------------------------------------
# Settings and the probe
# ---------------------------------------------------------------------------


def test_the_start_command_comes_from_the_manifest_and_a_bad_one_is_ignored_with_a_reason() -> None:
    assert embedder_settings({}) == EmbedderSettings()
    assert embedder_settings({"embedder": {"start_command": ["open", "-g", "-a", "Ollama"]}}) == START
    for section in ({"start_command": "open -g -a Ollama"}, {"start_command": ["open", ""]},
                    {"start_command": ["open", 1]}, "open"):
        settings = embedder_settings({"embedder": section})
        assert settings.start_command == () and settings.error


def test_the_probe_reads_which_models_ollama_holds_and_loads_nothing(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", "qwen3-embedding:0.6b")
    asked = []

    def answer(models):
        def urlopen(url, timeout):
            asked.append(url)
            return _Response(json.dumps({"models": models}).encode())
        return urlopen

    monkeypatch.setattr(embeddings.urllib.request, "urlopen", answer(
        [{"name": "qwen3-embedding:0.6b", "expires_at": "2026-10-28T20:06:24+01:00"}]))
    assert _REAL_PROBE() == LOADED
    monkeypatch.setattr(embeddings.urllib.request, "urlopen", answer([{"name": "llama3:8b"}]))
    assert _REAL_PROBE() == UNLOADED
    assert all(url.endswith("/api/ps") for url in asked)  # /api/ps never loads a model

    def refused(url, timeout):
        raise urllib.error.URLError(ConnectionRefusedError(61, "Connection refused"))

    monkeypatch.setattr(embeddings.urllib.request, "urlopen", refused)
    assert _REAL_PROBE() == DOWN


def test_a_model_named_without_a_tag_is_found_under_latest(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", "nomic-embed-text")
    monkeypatch.setattr(embeddings.urllib.request, "urlopen", lambda url, timeout: _Response(
        json.dumps({"models": [{"name": "nomic-embed-text:latest", "model": "nomic-embed-text:latest"}]}).encode()))

    assert _REAL_PROBE().loaded is True


@pytest.mark.parametrize("body", [b'{"models": 1}', b'{"models": "abc"}', b"[1, 2]", b"<html>"],
                         ids=["number", "string", "list", "html"])
def test_something_else_on_ollama_s_port_is_down(monkeypatch, body: bytes) -> None:
    monkeypatch.setattr(embeddings.urllib.request, "urlopen", lambda url, timeout: _Response(body))

    assert _REAL_PROBE() == DOWN


def test_models_ollama_cannot_describe_are_not_the_active_one(monkeypatch) -> None:
    monkeypatch.setattr(embeddings.urllib.request, "urlopen",
                        lambda url, timeout: _Response(b'{"models": [1, "x", {"name": null}]}'))

    assert _REAL_PROBE() == UNLOADED


def test_the_model_is_held_five_minutes_after_the_last_call(monkeypatch) -> None:
    """A load takes 0.7 s, so holding the model longer than a burst of work only costs memory."""
    monkeypatch.delenv("CHRONICLE_EMBED_KEEP_ALIVE", raising=False)
    sent = {}

    def urlopen(request, timeout):
        sent.update(json.loads(request.data))
        return _Response(json.dumps({"embeddings": [[1.0]]}).encode())

    monkeypatch.setattr(embeddings.urllib.request, "urlopen", urlopen)
    _REAL_EMBED_TEXT("x")

    assert embeddings.DEFAULT_KEEP_ALIVE == "5m" and sent["keep_alive"] == "5m"


# ---------------------------------------------------------------------------
# Starting Ollama
# ---------------------------------------------------------------------------


def test_a_running_ollama_is_left_alone(monkeypatch) -> None:
    _probe_sequence(monkeypatch, [UNLOADED])
    launched = _launches(monkeypatch)

    assert ensure_backend(START, wait_s=20.0) == UNLOADED and launched == []


def test_without_a_start_command_a_down_ollama_stays_down(monkeypatch) -> None:
    _probe_sequence(monkeypatch, [DOWN])
    launched = _launches(monkeypatch)

    assert ensure_backend(EmbedderSettings(), wait_s=20.0) == DOWN and launched == []
    assert embeddings.last_start() is None


def test_a_down_ollama_is_started_once_and_waited_for(monkeypatch) -> None:
    clock = _Clock()
    _probe_sequence(monkeypatch, [DOWN, DOWN, DOWN, UNLOADED])  # before, under the lock, two polls
    launched = _launches(monkeypatch)

    state = ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep)

    assert state == UNLOADED and launched == [START.start_command]
    started = embeddings.last_start()
    assert started["outcome"] == "up" and started["command"] == list(START.start_command)
    assert started["waited_s"] == 1.0  # two polls of START_POLL_S


def test_a_start_that_does_not_come_up_is_not_repeated_for_a_minute(monkeypatch) -> None:
    clock = _Clock()
    _probe_sequence(monkeypatch, [DOWN])
    launched = _launches(monkeypatch)

    assert ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep) == DOWN
    assert embeddings.last_start()["outcome"] == "not up in time" and embeddings.last_start()["waited_s"] == 20.0

    before = clock.now
    assert ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep) == DOWN
    assert clock.now == before and len(launched) == 1  # answered at once, nothing started

    clock.now += embeddings.START_RETRY_S
    ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep)
    assert len(launched) == 2


def test_a_start_command_still_running_is_not_run_again(monkeypatch) -> None:
    """An `ollama serve` that never answers must not pile up one process per minute."""
    clock = _Clock()
    _probe_sequence(monkeypatch, [DOWN])
    launched = []

    class _Running:
        def poll(self):
            return None

    monkeypatch.setattr(embeddings, "_launch", lambda command: launched.append(command) or _Running())

    ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep)
    clock.now += embeddings.START_RETRY_S
    ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep)

    assert len(launched) == 1 and embeddings.last_start()["ran_command"] is False


def test_a_hung_ollama_costs_no_more_than_the_wait(monkeypatch) -> None:
    """Every probe of a hung Ollama times out; the probes must fit in the wait too."""
    clock = _Clock()

    def hung(timeout=embeddings.PROBE_TIMEOUT_S):
        clock.now += timeout
        return DOWN

    monkeypatch.setattr(embeddings, "probe_backend", hung)
    _launches(monkeypatch)
    began = clock.now

    ensure_backend(START, wait_s=19.7, clock=clock, sleep=clock.sleep)

    assert clock.now - began <= 19.7 + 0.05


def test_a_command_that_cannot_run_is_reported_without_waiting(monkeypatch) -> None:
    clock = _Clock()
    _probe_sequence(monkeypatch, [DOWN])
    _launches(monkeypatch, fail=FileNotFoundError(2, "No such file or directory", "open"))

    assert ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep) == DOWN

    assert embeddings.last_start()["outcome"].startswith("failed: FileNotFoundError")
    assert embeddings.last_start()["waited_s"] == 0.0


def test_concurrent_searches_start_ollama_once(monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "START_POLL_S", 0.01)
    up = threading.Event()
    monkeypatch.setattr(embeddings, "probe_backend", lambda *a, **k: UNLOADED if up.is_set() else DOWN)
    launched = []

    def launch(command):
        launched.append(command)
        threading.Timer(0.1, up.set).start()

    monkeypatch.setattr(embeddings, "_launch", launch)
    states = []
    threads = [threading.Thread(target=lambda: states.append(ensure_backend(START, wait_s=5.0))) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(launched) == 1 and states == [UNLOADED] * 4


def test_the_real_launch_runs_the_command_detached(tmp_path) -> None:
    marker = tmp_path / "started"

    embeddings._launch(("/bin/sh", "-c", f"touch {marker}"))

    for _ in range(200):
        if marker.exists():
            break
        threading.Event().wait(0.01)
    assert marker.exists()


# ---------------------------------------------------------------------------
# Searches
# ---------------------------------------------------------------------------


def _timeouts(monkeypatch) -> list[float]:
    seen: list[float] = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, *, timeout: seen.append(timeout) or [1.0, 0.0])
    return seen


def test_a_query_to_a_loaded_model_keeps_the_short_timeout(monkeypatch) -> None:
    _probe_sequence(monkeypatch, [LOADED])
    seen = _timeouts(monkeypatch)

    assert embeddings.embed_query("anything", settings=START) == [1.0, 0.0]
    assert seen == [embeddings.DOCUMENT_TIMEOUT_S]


def test_a_query_to_an_unloaded_model_waits_for_it_to_load(monkeypatch) -> None:
    _probe_sequence(monkeypatch, [UNLOADED])
    seen = _timeouts(monkeypatch)

    embeddings.embed_query("anything", settings=START)

    assert embeddings.DOCUMENT_TIMEOUT_S < seen[0] <= embeddings.QUERY_WAIT_S == 20.0


def test_a_query_with_ollama_down_and_no_way_to_start_it_does_not_call_it(monkeypatch) -> None:
    _probe_sequence(monkeypatch, [DOWN])
    seen = _timeouts(monkeypatch)

    assert embeddings.embed_query("anything") is None and seen == []


def test_a_capture_holding_the_write_slot_does_not_wait_for_ollama(loaded_manifest, monkeypatch) -> None:
    manifest = {**loaded_manifest, "embedder": {"start_command": ["open", "-g", "-a", "Ollama"]}}
    asked = []
    monkeypatch.setattr(embeddings, "embed_query", lambda query, **options: asked.append(options) or None)

    service.build_startup_bundle(manifest, focus="backups", capture=True)
    service.build_startup_bundle(manifest, focus="backups", capture=False)

    held, free = asked
    assert held["wait_s"] == embeddings.DOCUMENT_TIMEOUT_S and held["settings"].start_command == ()
    assert free["wait_s"] == embeddings.QUERY_WAIT_S and free["settings"].start_command == START.start_command


def test_recall_starts_ollama_with_the_manifest_command(loaded_manifest, monkeypatch) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Backups go to T9"})
    _index_everything(loaded_manifest)
    manifest = {**loaded_manifest, "embedder": {"start_command": ["open", "-g", "-a", "Ollama"]}}
    monkeypatch.setattr(embeddings, "START_POLL_S", 0.0)
    _probe_sequence(monkeypatch, [DOWN, DOWN, LOADED])
    launched = _launches(monkeypatch)
    _timeouts(monkeypatch)

    response = service.query_memory(manifest, query="where do backups go")

    assert launched == [START.start_command]
    assert "vector" not in (response.get("channel_errors") or {})


# ---------------------------------------------------------------------------
# The background sweep
# ---------------------------------------------------------------------------


def _index_everything(manifest: dict) -> None:
    config = config_from_manifest(manifest)
    key = embeddings.active_profile().key
    for event_id in fetch_event_ids_without_embedding(config, model_key=key):
        store_event_embedding(config, event_id, [1.0, 0.0], key, 2)


def test_an_idle_server_sweep_leaves_ollama_alone(loaded_manifest, monkeypatch) -> None:
    """The 2026-10-28 cause: a dimension probe every 5 minutes renewed the model's 30 minutes forever."""
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Indexed already"})
    _index_everything(loaded_manifest)
    probes = _probe_sequence(monkeypatch, [LOADED])
    embedded = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: embedded.append(text))

    report = service.embed_backfill(loaded_manifest, limit=100, lazy=True)

    assert probes == [] and embedded == []
    assert (report["total_without_embedding"], report["backend_unavailable"]) == (0, False)


def test_a_sweep_with_work_starts_ollama_and_embeds(loaded_manifest, monkeypatch) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Needs a vector"})
    manifest = {**loaded_manifest, "embedder": {"start_command": ["open", "-g", "-a", "Ollama"]}}
    monkeypatch.setattr(embeddings, "START_POLL_S", 0.0)
    _probe_sequence(monkeypatch, [DOWN, DOWN, UNLOADED])
    launched = _launches(monkeypatch)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: [0.6, 0.8])

    report = service.embed_backfill(manifest, limit=100, lazy=True)

    assert launched == [START.start_command] and report["embedded"] >= 1 and not report["backend_unavailable"]


def test_a_sweep_with_ollama_down_skips_the_probe(loaded_manifest, monkeypatch) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Needs a vector"})
    _probe_sequence(monkeypatch, [DOWN])
    calls = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: calls.append(text))

    report = service.embed_backfill(loaded_manifest, limit=100, lazy=True)

    assert calls == [] and report["backend_unavailable"] is True


def test_a_backfill_from_the_cli_still_checks_the_model_s_dimension(loaded_manifest, monkeypatch) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Indexed already"})
    _index_everything(loaded_manifest)
    calls = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: calls.append(text) or [1.0, 0.0])

    service.embed_backfill(loaded_manifest)

    assert calls == [embeddings.DIMENSION_PROBE_TEXT]


def test_the_server_s_embedder_sweeps_lazily(chronicle_sandbox, loaded_manifest, monkeypatch) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Indexed already"})
    _index_everything(loaded_manifest)
    monkeypatch.setattr(mcp_server.BackgroundEmbedder, "start", lambda self: None)
    probes = _probe_sequence(monkeypatch, [LOADED])
    embedded = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: embedded.append(text))

    mcp_server._start_embedder(chronicle_sandbox.manifest_path).run_once()

    assert probes == [] and embedded == []


def test_the_daily_capture_with_a_complete_index_leaves_ollama_alone(loaded_manifest, loaded_automation,
                                                                       monkeypatch) -> None:
    from max_chronicle.native_automation import run_automation_job

    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Indexed already"})
    _index_everything(loaded_manifest)
    probes = _probe_sequence(monkeypatch, [LOADED])
    embedded = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: embedded.append(text))

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")

    assert result["status"] == "ok" and probes == [] and embedded == []


def test_notes_with_nothing_to_embed_leave_ollama_alone(loaded_manifest, monkeypatch) -> None:
    from max_chronicle.notes import embed_chunks

    probes = _probe_sequence(monkeypatch, [LOADED])

    assert embed_chunks(loaded_manifest)["missing"] == 0 and probes == []


def test_notes_with_ollama_down_do_not_call_it(loaded_manifest, monkeypatch) -> None:
    from max_chronicle import notes

    config = config_from_manifest(loaded_manifest)
    with notes.write_transaction(config) as connection:
        connection.execute("INSERT INTO documents(id, path, source, project, title, content_sha256, size_bytes, "
                           "modified_at_utc, indexed_at_utc) VALUES ('d1', '/n.md', 'claude-memory', 'status', 'n', "
                           "'h', 1, '2026-10-28T00:00:00Z', '2026-10-28T00:00:00Z')")
        connection.execute("INSERT INTO document_chunks(id, document_id, ordinal, heading, text) "
                           "VALUES ('c1', 'd1', 0, 'Rule', 'Back up to T9')")
    _probe_sequence(monkeypatch, [DOWN])
    calls = []
    monkeypatch.setattr(embeddings, "embed_documents", lambda texts, **options: calls.append(texts))

    report = notes.embed_chunks(loaded_manifest)

    assert calls == [] and (report["missing"], report["failed"]) == (1, 1)


# ---------------------------------------------------------------------------
# What the operator sees
# ---------------------------------------------------------------------------


def test_doctor_says_whether_ollama_is_up_and_how_it_starts(loaded_manifest, loaded_automation, monkeypatch) -> None:
    manifest = {**loaded_manifest, "embedder": {"start_command": ["open", "-g", "-a", "Ollama"]}}
    _probe_sequence(monkeypatch, [DOWN])

    runtime = doctor.runtime_component(manifest, loaded_automation, server_url=NOBODY)

    assert runtime["embedder"]["up"] is False and runtime["embedder"]["start_command"] == ["open", "-g", "-a", "Ollama"]
    text = doctor.render_summary({"status": "ok", "components": {"runtime": runtime}})
    assert "ollama     down · started on demand by `open -g -a Ollama`" in text


def test_a_malformed_start_command_is_a_doctor_warning(loaded_manifest, loaded_automation) -> None:
    manifest = {**loaded_manifest, "embedder": {"start_command": "open -a Ollama"}}

    runtime = doctor.runtime_component(manifest, loaded_automation, server_url=NOBODY)

    assert "manifest: embedder.start_command must be a list of non-empty strings" in runtime["warnings"]


@pytest.mark.parametrize(("embedder", "server", "expected"), [
    ({"up": True, "model": "m", "model_loaded": True, "expires_at": "20:06"}, {},
     "ollama     up · m loaded until 20:06"),
    ({"up": True, "model": "m", "model_loaded": False}, {}, "ollama     up · m not loaded"),
    ({"up": False, "model": "m", "start_command": None}, {},
     "ollama     down · no start command: search runs on text only"),
    ({"up": True, "model": "m", "model_loaded": False},
     {"answering": True, "embedder": {"last_start": {"at": "2026-10-28T19:00:00Z", "outcome": "up"}}},
     "ollama     up · m not loaded · last start 2026-10-28T19:00:00Z: up"),
], ids=["loaded", "unloaded", "down", "started"])
def test_the_summary_has_one_ollama_line(embedder, server, expected) -> None:
    payload = {"status": "ok", "components": {"runtime": {"package_version": "9", "server": server,
                                                          "embedder": embedder}}}

    assert expected in doctor.render_summary(payload).splitlines()


def test_health_reports_the_last_start(monkeypatch) -> None:
    clock = _Clock()
    _probe_sequence(monkeypatch, [DOWN, DOWN, UNLOADED])
    _launches(monkeypatch)
    ensure_backend(START, wait_s=20.0, clock=clock, sleep=clock.sleep)
    embedder = mcp_server.BackgroundEmbedder(dict, lambda manifest, **kwargs: {"embedded": 0})
    monkeypatch.setitem(mcp_server._RUNTIME, "embedder", embedder)

    health = mcp_server._runtime_health()

    assert health["embedder"]["last_start"]["outcome"] == "up"
