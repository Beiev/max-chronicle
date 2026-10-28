"""Chronicle embeddings — local Ollama-backed text embeddings with pure-Python cosine.

All public functions degrade gracefully when Ollama is unavailable.  NEVER raise
on network or parsing errors — callers treat None as "skip this channel".

Ollama runs on demand. The model stays loaded for ``keep_alive`` after the last
call and Ollama unloads it after that; nothing calls it without work to do. When
``[embedder] start_command`` is set in the manifest, a search or a background
sweep that finds Ollama down starts it with that command and waits for it.

Each vector is stored under the key of the profile that produced it. Recall
compares a query only with vectors of the active profile, so an index built by
one model is never ranked against another model's query, and a new model can be
backfilled while the old index still serves.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.request
from typing import Any

_LOGGER = logging.getLogger("max_chronicle.embeddings")

# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

ENV_EMBED_MODEL = "CHRONICLE_EMBED_MODEL"
DEFAULT_EMBED_MODEL = "qwen3-embedding:0.6b"
DOCUMENT_TIMEOUT_S = 3.0  # on the write path, outside the write lock, and a query to a loaded model
BACKFILL_TIMEOUT_S = 30.0  # a long event on a cold model
FLOAT32_MAX = 3.4028234663852886e38  # vectors are stored as float32
DIMENSION_PROBE_TEXT = "dimension probe"  # embedded once per backfill to learn the model's dimension
# How long Ollama keeps the model loaded after the last call, then unloads it
# (2.9 GB for qwen3-embedding:0.6b at num_ctx 8192). A load took 0.7 s with free
# memory on 2026-10-28; the first query after an unload waits for it
# (QUERY_WAIT_S), so the model is not held between bursts of work.
ENV_KEEP_ALIVE = "CHRONICLE_EMBED_KEEP_ALIVE"
DEFAULT_KEEP_ALIVE = "5m"
# The context window Ollama allocates for the model. Its default for
# qwen3-embedding is the model's 32k, about 5.4 GB resident for events and note
# sections that stay far below that; 8192 needs about half and gives identical
# vectors for any shorter text (cosine 1.0). Longer text is truncated, as
# `truncate` already allows.
ENV_NUM_CTX = "CHRONICLE_EMBED_NUM_CTX"
DEFAULT_NUM_CTX = 8192

_DEFAULT_OLLAMA_URL = "http://localhost:11434"

# Ollama on demand.
QUERY_WAIT_S = 20.0  # a search waits this long for Ollama to start and load the model
BACKGROUND_START_WAIT_S = 60.0  # nobody waits on a background sweep
START_RETRY_S = 60.0  # after a start that did not bring Ollama up, answer without it this long
START_POLL_S = 0.5
PROBE_TIMEOUT_S = 1.0  # /api/ps on the local machine answers in milliseconds


@dataclass(frozen=True)
class EmbeddingProfile:
    """How one model embeds documents and queries."""

    key: str  # stored with every vector; change it when stored vectors stop being comparable
    model: str  # Ollama model tag
    query_prefix: str = ""
    document_prefix: str = ""
    # Cosine floor for a candidate found by the vector channel alone. Similarity
    # scales differ by model, so each profile carries its own.
    min_similarity: float = 0.65
    # Cosine at which a vector match is confident evidence (FR-2), or None when
    # this model's similarity does not tell relevant from unrelated text.
    confident_similarity: float | None = None


# Qwen3-Embedding is instruction-aware on the query side; documents are raw.
_QWEN3_QUERY = (
    "Instruct: Given a question about past work, decisions and events, "
    "retrieve the memory entries that answer it\nQuery:"
)
# Floors were measured with `chronicle eval` on 64 real questions (54 answerable,
# 10 that memory cannot answer). qwen3-embedding:0.6b: ranking peaks at a 0.45-0.50
# floor; 0.60 flags 8 of 10 unanswerable questions and 15 of 54 answerable ones
# as unconfident. nomic-embed-text: its top similarity does not separate the two
# groups at all (AUC 0.50), so only a lexical match makes it confident.
PROFILES = {
    profile.model: profile
    for profile in (
        # Multilingual (Russian, Ukrainian, English), 32K context, 1024 dimensions.
        EmbeddingProfile(key="qwen3-embedding:0.6b", model="qwen3-embedding:0.6b", query_prefix=_QWEN3_QUERY,
                         min_similarity=0.5, confident_similarity=0.6),
        # Not yet measured: calibrate both floors with `chronicle eval` before relying on it.
        EmbeddingProfile(key="qwen3-embedding:4b", model="qwen3-embedding:4b", query_prefix=_QWEN3_QUERY,
                         min_similarity=0.5),
        # English only; kept so an existing index stays usable. Its vectors were
        # stored without the search_document prefix, so none is added now.
        EmbeddingProfile(key="nomic-embed-text", model="nomic-embed-text", min_similarity=0.65),
    )
}


def active_profile() -> EmbeddingProfile:
    """The profile named by CHRONICLE_EMBED_MODEL, or a plain one for an unknown model."""
    model = os.environ.get(ENV_EMBED_MODEL, "").strip() or DEFAULT_EMBED_MODEL
    return PROFILES.get(model) or EmbeddingProfile(key=model, model=model)


def _ollama_base_url() -> str:
    return os.environ.get("OLLAMA_URL", _DEFAULT_OLLAMA_URL).rstrip("/")


@dataclass(frozen=True)
class EmbedderSettings:
    """The manifest's [embedder] section."""

    start_command: tuple[str, ...] = ()  # argv that starts Ollama; empty: never start it
    error: str | None = None  # why the section was ignored


def embedder_settings(manifest: dict[str, Any] | None) -> EmbedderSettings:
    """Read [embedder]; a malformed section is ignored and says why. NEVER raises."""
    section = (manifest or {}).get("embedder")
    if section is None:
        return EmbedderSettings()
    if not isinstance(section, dict):
        return EmbedderSettings(error="[embedder] must be a table")
    command = section.get("start_command", [])
    if not isinstance(command, list) or not all(isinstance(part, str) and part.strip() for part in command):
        return EmbedderSettings(error="embedder.start_command must be a list of non-empty strings")
    return EmbedderSettings(start_command=tuple(command))


def _keep_alive() -> str:
    return os.environ.get(ENV_KEEP_ALIVE, "").strip() or DEFAULT_KEEP_ALIVE


def _num_ctx() -> int:
    raw = os.environ.get(ENV_NUM_CTX, "").strip()
    try:
        value = int(raw) if raw else DEFAULT_NUM_CTX
    except ValueError:
        return DEFAULT_NUM_CTX
    return value if value > 0 else DEFAULT_NUM_CTX


# ---------------------------------------------------------------------------
# Vector utilities
# ---------------------------------------------------------------------------


def pack_vector(vec: list[float]) -> bytes:
    """Pack a list of float32 values into a raw BLOB."""
    return struct.pack(f"{len(vec)}f", *vec)


def unpack_vector(blob: bytes) -> list[float]:
    """Unpack a raw BLOB back to a list of float32 values."""
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


_sumprod = getattr(math, "sumprod", None)  # C speed on Python 3.12+


def _dot(a: list[float], b: list[float]) -> float:
    return _sumprod(a, b) if _sumprod is not None else sum(x * y for x, y in zip(a, b))


def similarity_scorer(query: list[float]) -> Callable[[list[float]], float]:
    """Cosine similarity to *query* for many vectors, the query's norm computed once.

    A vector of another dimension or with a non-finite value raises ValueError;
    a zero vector scores 0.0.
    """
    query_norm = math.sqrt(_dot(query, query))
    if not math.isfinite(query_norm):
        raise ValueError("Embedding contains non-finite values")

    def score(vector: list[float]) -> float:
        if len(vector) != len(query):
            raise ValueError("Embedding dimensions do not match")
        dot = _dot(query, vector)
        norm = math.sqrt(_dot(vector, vector))
        if not (math.isfinite(dot) and math.isfinite(norm)):
            raise ValueError("Embedding contains non-finite values")
        if norm == 0.0 or query_norm == 0.0:
            return 0.0
        return dot / (query_norm * norm)

    return score


def cosine(a: list[float], b: list[float]) -> float:
    """Return the cosine similarity between two equal-length vectors.

    Returns 0.0 when either vector is zero-length (degenerate embedding).
    """
    return similarity_scorer(a)(b)


# ---------------------------------------------------------------------------
# Ollama on demand
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackendState:
    """What Ollama answered about itself."""

    up: bool
    loaded: bool = False  # the active profile's model is in memory
    expires_at: str | None = None  # when Ollama unloads it


def _model_tag(name: str) -> str:
    """Ollama's full name for a model: a name without a tag means ``:latest``."""
    return name if ":" in name.rsplit("/", 1)[-1] else f"{name}:latest"


def probe_backend(timeout: float = PROBE_TIMEOUT_S) -> BackendState:
    """Ask Ollama which models it holds; down on any failure. Loads nothing. NEVER raises."""
    try:
        with urllib.request.urlopen(f"{_ollama_base_url()}/api/ps", timeout=timeout) as resp:
            models = json.loads(resp.read()).get("models") or []
        if not isinstance(models, list):
            return BackendState(up=False)  # something else answers on Ollama's port
        model = _model_tag(active_profile().model)
        held = next((item for item in models if isinstance(item, dict)
                     and model in {_model_tag(str(item.get(key) or "")) for key in ("name", "model")}), None)
        return BackendState(up=True, loaded=held is not None, expires_at=(held or {}).get("expires_at"))
    except Exception:  # noqa: BLE001 — refused, hung or garbled: all mean "not usable now"
        return BackendState(up=False)


_START_LOCK = threading.Lock()
_START_STATE: dict[str, Any] = {"last_start": None, "failed_at": None, "process": None}


def last_start() -> dict[str, Any] | None:
    """The last time this process started Ollama, for /health; None if it never did."""
    return _START_STATE["last_start"]


def reset_start_state() -> None:
    """Forget earlier starts (tests)."""
    _START_STATE.update(last_start=None, failed_at=None, process=None)


def _launch(command: tuple[str, ...]) -> subprocess.Popen:
    """Run the start command detached; a long-running one (``ollama serve``) outlives this call."""
    process = subprocess.Popen(list(command), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, start_new_session=True)
    threading.Thread(target=process.wait, name="chronicle-ollama-start", daemon=True).start()
    return process


def ensure_backend(settings: EmbedderSettings, *, wait_s: float,
                   clock: Callable[[], float] = time.monotonic,
                   sleep: Callable[[float], None] = time.sleep) -> BackendState:
    """Ollama's state, starting it first when it is down and a start command is set. NEVER raises.

    Everything fits in *wait_s*: the probes, the wait for another thread's start
    and the polls. One thread starts Ollama while the others wait for the same
    result. A start command that is still running (an ``ollama serve`` that
    never answers) is not run again, and a start that did not bring Ollama up
    is neither repeated nor waited for again for START_RETRY_S.
    """
    deadline = clock() + wait_s

    def probe() -> BackendState:
        return probe_backend(timeout=max(0.05, min(PROBE_TIMEOUT_S, deadline - clock())))

    state = probe()
    if state.up or not settings.start_command:
        return state
    if not _START_LOCK.acquire(timeout=max(0.0, deadline - clock())):
        return probe()  # the thread that holds it may have brought Ollama up
    try:
        state = probe()  # another thread may have started it meanwhile
        failed_at = _START_STATE["failed_at"]
        if state.up or (failed_at is not None and clock() - failed_at < START_RETRY_S):
            return state
        started = clock()
        record: dict[str, Any] = {"at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                  "command": list(settings.start_command)}
        running = _START_STATE["process"]
        try:
            record["ran_command"] = running is None or running.poll() is not None
            if record["ran_command"]:
                _START_STATE["process"] = _launch(settings.start_command)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            record["outcome"] = f"failed: {type(exc).__name__}: {exc}"
        else:
            while not state.up and clock() < deadline:
                sleep(max(0.0, min(START_POLL_S, deadline - clock())))
                state = probe()
            record["outcome"] = "up" if state.up else "not up in time"
        record["waited_s"] = round(clock() - started, 1)
        _START_STATE.update(last_start=record, failed_at=None if state.up else clock())
        (_LOGGER.info if state.up else _LOGGER.warning)("Ollama start: %s", record)
        return state
    finally:
        _START_LOCK.release()


def backend_status(manifest: dict[str, Any] | None) -> dict[str, Any]:
    """Ollama as `chronicle doctor` reports it: asks it now, starts nothing. NEVER raises."""
    settings = embedder_settings(manifest)
    state = probe_backend()
    return {"url": _ollama_base_url(), "model": active_profile().model, "up": state.up,
            "model_loaded": state.loaded, "expires_at": state.expires_at, "keep_alive": _keep_alive(),
            "start_command": list(settings.start_command) or None, "settings_error": settings.error}


# ---------------------------------------------------------------------------
# Ollama embedding call
# ---------------------------------------------------------------------------


def event_embedding_text(event: dict[str, Any]) -> str:
    parts = [event.get(key) for key in ("project", "text", "why")]
    if event.get("fact"):
        parts.extend(event["fact"].get(key) for key in ("slot", "value"))
    if event.get("checkpoint"):
        parts.append(event["checkpoint"].get("goal"))
    return " ".join(part for part in parts if part).strip()


def _request_embeddings(inputs: str | list[str], *, timeout: float) -> list[list[float]] | None:
    """One Ollama /api/embed call for a text or a list; None unless every vector is usable. NEVER raises."""
    expected = 1 if isinstance(inputs, str) else len(inputs)
    try:
        payload = {"model": active_profile().model, "input": inputs, "truncate": True,
                   "keep_alive": _keep_alive(), "options": {"num_ctx": _num_ctx()}}
        req = urllib.request.Request(
            f"{_ollama_base_url()}/api/embed",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
        embeddings = json.loads(body).get("embeddings")
        if not isinstance(embeddings, list) or len(embeddings) != expected:
            return None
        vectors = []
        for item in embeddings:
            if not isinstance(item, list):
                return None
            vector = [float(x) for x in item]
            if not vector or not all(math.isfinite(x) for x in vector) or not any(vector):
                return None
            vectors.append(vector)
        return vectors
    except Exception:  # noqa: BLE001 — intentional blanket catch
        return None


def embed_text(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed *text* as given with the active profile's model.

    Uses Ollama's /api/embed with truncation, so a text longer than the model's
    context is embedded from its start instead of failing. The dimension is the
    model's own. Returns None on ANY failure (connection error, timeout, bad
    response). NEVER raises.
    """
    vectors = _request_embeddings(text, timeout=timeout)
    return vectors[0] if vectors else None


def embed_documents(texts: list[str], *, timeout: float = BACKFILL_TIMEOUT_S) -> list[list[float]] | None:
    """Embed stored texts (note chunks) in one call; None on any failure. NEVER raises."""
    if not texts:
        return []
    return _request_embeddings([active_profile().document_prefix + text for text in texts], timeout=timeout)


def embed_query(query: str, *, settings: EmbedderSettings | None = None,
                wait_s: float = QUERY_WAIT_S) -> list[float] | None:
    """Embed a search query the way the active profile expects.

    Waits up to *wait_s* for Ollama to start (with a start command) and an
    unloaded model to load, giving the load at least DOCUMENT_TIMEOUT_S. A loaded
    model gets DOCUMENT_TIMEOUT_S, as before, so a hung backend still costs a
    query only that.
    """
    deadline = time.monotonic() + wait_s
    state = ensure_backend(settings or EmbedderSettings(), wait_s=wait_s)
    if not state.up:
        return None
    timeout = DOCUMENT_TIMEOUT_S if state.loaded else max(DOCUMENT_TIMEOUT_S, deadline - time.monotonic())
    return embed_text(active_profile().query_prefix + query, timeout=timeout)


def embed_document(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed stored text (an event) the way the active profile expects."""
    return embed_text(active_profile().document_prefix + text, timeout=timeout)
