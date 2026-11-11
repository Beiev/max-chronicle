"""The Atlas: the whole memory as one graph, for the map at ``/ui/atlas``.

Nodes are projects, tasks, events, notes and fact slots. Edges are what the
memory itself records: an event belongs to a task or a project, a task to its
project, a note to its project, a note links another note (``[[name]]`` or a
markdown link), an event set a value of a fact slot. When vectors exist, the
nearest neighbours by meaning are a separate, weaker kind of edge. The first
pass compares every pair, so a process of its own computes them and keeps
them in a file; later passes compare only what was added or changed.

Texts (an event's words, a note's title, a fact's value, a record opened in
full) are redacted again on the way out; names (projects, tasks, fact slots)
are shown as recorded, as everywhere in Chronicle. The page renders all of it
as text, never as markup. Quarantined events (the event, or every one of its
observations), deleted notes and copies hidden in the Atlas never appear;
neither do the Atlas's own events, whose actions show on the memories they
were taken on (``curation``).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime, timezone
import array
import heapq
from itertools import repeat
import json
import math
import operator
import os
from pathlib import Path, PurePath
import re
import signal
import site
import subprocess
import sys
import threading
import time
from typing import Any
import zlib

from . import curation
from .agent_surface import get_items, gist
from .brief import _read_only
from .embeddings import active_profile
from .memory import open_tasks
from .redaction import redact, redact_value
from .service import _UNREDACTED_ENTRY_KEYS
from .store import config_from_manifest

EDGE_KINDS = ("task", "project", "link", "fact", "similar")
TASK, PROJECT, LINK, FACT, SIMILAR = range(len(EDGE_KINDS))
LABEL_CHARS = 140
# Neighbours by meaning kept per memory, and the weakest resemblance kept: the
# nearest neighbour of a typical event sits at 0.85 with qwen3-embedding.
SIMILAR_K = 3
SIMILAR_MIN = 0.80
# Above this, two memories say the same thing: the page offers to hide one as a copy.
DUPLICATE_MIN = curation.DUPLICATE_MIN
# The cache of neighbours: its format, and when an update is a fresh pass anyway.
SIMILAR_VERSION = 1
INCREMENTAL_SHARE = 0.4
SIMILAR_TIMEOUT_S = 20 * 60
SIMILAR_RETRY_S = 5 * 60
SIMILAR_LOG_BYTES = 1 << 20
OPEN_TASK_LIMIT = 10_000
_WIKILINK = re.compile(r"\[\[([^\]|#\n]+)")
_MD_LINK = re.compile(r"\]\((?:\./)?([^)\s#]+\.md)\)")
_DATED_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})[-_]")
_NOT_ATLAS = f"e.category IS NOT '{curation.CATEGORY}'"
_VISIBLE = f"e.visibility != 'raw_only' AND o.visibility != 'raw_only' AND {_NOT_ATLAS}"
# An event is on the map when neither it nor every one of its observations is
# quarantined, and it is not one of the Atlas's own.
_SHOWN_EVENT = ("e.visibility != 'raw_only' AND EXISTS (SELECT 1 FROM event_observations o "
                f"WHERE o.event_id = e.id AND o.visibility != 'raw_only') AND {_NOT_ATLAS}")
# Identifiers and paths of a record: never free text, so never redacted (as when it was written).
_ITEM_IDENTIFIERS = _UNREDACTED_ENTRY_KEYS | {"path", "event_id", "observation_id", "pinned", "stale"}

try:
    _dot = math.sumprod  # Python 3.12+: one C loop per pair
except AttributeError:  # pragma: no cover - Python 3.11
    def _dot(a: Sequence[float], b: Sequence[float]) -> float:
        return sum(map(operator.mul, a, b))


def _epoch(value: str | None) -> int | None:
    if not value:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def _label(text: str | None, limit: int = LABEL_CHARS) -> str:
    return gist(redact(text or "").text, limit)[0] or ""


class _Graph:
    """Nodes in insertion order, addressed by a stable string id."""

    def __init__(self) -> None:
        self.nodes: list[dict[str, Any]] = []
        self.index: dict[str, int] = {}
        self.edges: list[list[Any]] = []
        self.projects: list[str] = []
        self._project_index: dict[str, int] = {}

    def project_index(self, name: str | None) -> int | None:
        if not name:
            return None
        if name not in self._project_index:
            self._project_index[name] = len(self.projects)
            self.projects.append(name)
        return self._project_index[name]

    def add(self, node_id: str, kind: str, label: str, project: str | None, when: int | None,
            **extra: Any) -> int:
        """The node's position; a node seen again keeps its first time ``t`` and its last ``u``."""
        if node_id in self.index:
            position = self.index[node_id]
            node = self.nodes[position]
            if when is not None:
                node["t"] = when if node["t"] is None else min(node["t"], when)
                node["u"] = when if node["u"] is None else max(node["u"], when)
            return position
        self.index[node_id] = len(self.nodes)
        self.nodes.append({"id": node_id, "k": kind, "l": label, "p": self.project_index(project),
                           "t": when, "u": when,
                           **{key: value for key, value in extra.items() if value is not None}})
        return self.index[node_id]

    def link(self, source: int, target: int, kind: int) -> None:
        if source != target:
            self.edges.append([source, target, kind])


def _project_node(graph: _Graph, name: str, when: int | None) -> int:
    return graph.add(f"p:{name}", "project", name, name, when)


def _task_key(project: str | None, task_id: str) -> str:
    return f"t:{project or ''}/{task_id.casefold()}"


def _flags(marks: curation.Marks, target: str) -> dict[str, int | None]:
    """The marks a node carries: ``pi`` pinned to the brief, ``st`` stale."""
    return {"pi": 1 if target in marks.pinned else None, "st": 1 if target in marks.stale else None}


def _events(connection, identities, graph: _Graph, marks: curation.Marks) -> None:
    rows = connection.execute(
        f"""SELECT e.id, e.recorded_at_utc, e.actor, e.category, e.text, o.project, o.task_id, MAX(o.seq) AS last_seq
        FROM events e JOIN event_observations o ON o.event_id = e.id
        WHERE {_VISIBLE} GROUP BY e.id ORDER BY e.recorded_at_utc""").fetchall()
    open_keys = {_task_key(task["project"], task["task_id"])
                 for task in open_tasks(connection, identities, limit=OPEN_TASK_LIMIT)}
    for row in rows:
        when = _epoch(row["recorded_at_utc"])
        project = identities.project(row["project"]) or row["project"]
        event = graph.add(f"e:{row['id']}", "event", _label(row["text"]), project, when,
                          c=row["category"], a=row["actor"], **_flags(marks, f"event:{row['id']}"))
        parent = _project_node(graph, project, when) if project else None
        if row["task_id"]:
            key = _task_key(project, row["task_id"])
            task = graph.add(key, "task", row["task_id"], project, when, o=1 if key in open_keys else None)
            if task == len(graph.nodes) - 1 and parent is not None:
                graph.link(task, parent, PROJECT)
            graph.link(event, task, TASK)
        elif parent is not None:
            graph.link(event, parent, PROJECT)


def _note_targets(texts: Iterable[str]) -> set[str]:
    targets: set[str] = set()
    for text in texts:
        targets.update(match.strip().casefold() for match in _WIKILINK.findall(text))
        targets.update(PurePath(match).stem.casefold() for match in _MD_LINK.findall(text))
    return targets


def _notes(connection, identities, graph: _Graph, marks: curation.Marks) -> None:
    documents = connection.execute(
        """SELECT id, path, project, title, kind, modified_at_utc FROM documents
        WHERE deleted_at_utc IS NULL ORDER BY modified_at_utc""").fetchall()
    hidden = marks.hidden_notes()
    by_name: dict[str, int] = {}
    for document in documents:
        if document["id"] in hidden:
            continue
        # An imported archive is modified on the day of the import; a note named
        # after its day (2026-06-18-….md) is dated by its name.
        dated = _DATED_NAME.match(PurePath(document["path"]).name)
        when = _epoch(f"{dated[1]}T12:00:00Z" if dated else document["modified_at_utc"])
        project = identities.project(document["project"]) or document["project"]
        note = graph.add(f"n:{document['id']}", "note", _label(document["title"]), project, when,
                         c=document["kind"], **_flags(marks, f"note:{document['id']}"))
        by_name.setdefault(PurePath(document["path"]).stem.casefold(), note)
        by_name.setdefault((document["title"] or "").strip().casefold(), note)
        if project:
            graph.link(note, _project_node(graph, project, when), PROJECT)
    chunks: dict[str, list[str]] = {}
    for chunk in connection.execute(
            "SELECT document_id, text FROM document_chunks WHERE text LIKE '%[[%' OR text LIKE '%.md)%'"):
        chunks.setdefault(chunk["document_id"], []).append(chunk["text"])
    seen: set[tuple[int, int]] = set()
    for document_id, texts in chunks.items():
        source = graph.index.get(f"n:{document_id}")
        if source is None:
            continue
        for name in _note_targets(texts):
            target = by_name.get(name)
            if target is not None and target != source and (source, target) not in seen:
                seen.add((source, target))
                graph.link(source, target, LINK)


def _facts(connection, identities, graph: _Graph) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""SELECT f.id, f.domain, f.slot_key, f.value_key, f.project, f.task_id, f.recorded_at_utc,
                  f.expired_at_utc, f.event_id, f.kind
        FROM facts f JOIN events e ON e.id = f.event_id
        WHERE {_SHOWN_EVENT} AND f.status NOT IN ('redacted', 'purged')
          AND NOT EXISTS (SELECT 1 FROM event_observations q WHERE q.id = f.observation_id
                          AND q.visibility = 'raw_only')
        ORDER BY f.recorded_at_utc""").fetchall()
    history = []
    for row in rows:
        when = _epoch(row["recorded_at_utc"])
        project = identities.project(row["project"]) or row["project"]
        scope = f"{row['domain'] or ''}|{project or ''}|{(row['task_id'] or '').casefold()}"
        slot = graph.add(f"f:{row['slot_key']}|{scope}", "fact", row["slot_key"], project, when)
        event = graph.index.get(f"e:{row['event_id']}")
        if event is not None:
            graph.link(slot, event, FACT)
        history.append({"node": slot, "value": _label(row["value_key"]), "from": when,
                        "to": _epoch(row["expired_at_utc"]), "kind": row["kind"]})
    return history


def build_graph(connection, identities) -> dict[str, Any]:
    """The map's nodes, recorded edges and fact history, without the edges by meaning."""
    graph = _Graph()
    marks = curation.marks(connection)
    _events(connection, identities, graph, marks)
    _notes(connection, identities, graph, marks)
    facts = _facts(connection, identities, graph)
    times = [node["t"] for node in graph.nodes if node["t"] is not None]
    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "span": [min(times), max(times)] if times else None,
        "projects": graph.projects,
        "edge_kinds": list(EDGE_KINDS),
        "nodes": graph.nodes,
        "edges": graph.edges,
        "facts": facts,
    }




# ---------------------------------------------------------------------------
# Edges by meaning: each memory's nearest neighbours, kept in a cache file
# ---------------------------------------------------------------------------

Neighbours = dict[str, list[tuple[float, str]]]


def similar_cache_path(config) -> Path:
    return config.artifact_dir / "atlas" / "similar.json"


def _vector_rows(connection, model_key: str) -> Iterator[tuple[str, bytes]]:
    """The node id and vector of every shown event, then of every section of a shown note, in order."""
    hidden = {f"n:{key}" for key in curation.marks(connection).hidden_notes()}
    yield from connection.execute(
        f"""SELECT 'e:' || v.event_id, v.vector FROM event_vectors v JOIN events e ON e.id = v.event_id
        WHERE v.model_key = ? AND {_SHOWN_EVENT}""", (model_key,))
    for key, blob in connection.execute(
            """SELECT 'n:' || c.document_id, v.vector FROM chunk_vectors v
            JOIN document_chunks c ON c.id = v.chunk_id JOIN documents d ON d.id = c.document_id
            WHERE v.model_key = ? AND d.deleted_at_utc IS NULL ORDER BY c.document_id, c.ordinal""", (model_key,)):
        if key not in hidden:
            yield key, blob


def vector_digest(connection, model_key: str) -> tuple[list[Any], set[str]]:
    """A signature of every vector the map compares, and the memories they belong to.

    A checksum of each vector with its memory: a vector added, replaced in
    place or removed, a memory quarantined or shown again, a note deleted,
    each changes it. Timestamps would not do: they have a resolution of a
    second. It reads every vector (about 35 ms for 20 MB).
    """
    count = total = 0
    keys: set[str] = set()
    for key, blob in _vector_rows(connection, model_key):
        count += 1
        total += zlib.crc32(blob, zlib.crc32(key.encode()))
        keys.add(key)
    return [model_key, count, total], keys


def _unpack(blob: bytes) -> list[float]:
    values = array.array("f")
    values.frombytes(blob)
    return values.tolist()


def memory_vectors(connection, model_key: str) -> tuple[dict[str, list[float]], dict[str, int]]:
    """One unit vector per shown event and live note (the mean of its sections), and a checksum of what it is made of."""
    raw: dict[str, list[float]] = {}
    checksums: dict[str, int] = {}
    for key, blob in _vector_rows(connection, model_key):
        values = _unpack(blob)
        if key in raw:  # another section of the same note
            if len(values) != len(raw[key]):
                continue
            raw[key] = [a + b for a, b in zip(raw[key], values)]
        else:
            raw[key] = values
        checksums[key] = zlib.crc32(blob, checksums.get(key, 0))
    if not raw:
        return {}, {}
    sizes: dict[int, int] = {}
    for values in raw.values():
        sizes[len(values)] = sizes.get(len(values), 0) + 1
    size = max(sizes, key=sizes.__getitem__)  # one model has one dimension; anything else is damage
    vectors: dict[str, list[float]] = {}
    for key, values in raw.items():
        norm = math.sqrt(_dot(values, values)) if len(values) == size else 0.0
        if norm:
            vectors[key] = [value / norm for value in values]
    return vectors, {key: checksums[key] for key in vectors}


def _memory_vector(connection, model_key: str, target: str) -> list[float] | None:
    """The unit vector the map compares for one memory (``event:<id>``/``note:<id>``), made as memory_vectors makes it."""
    kind, _, key = target.partition(":")
    if kind == "event":
        rows = connection.execute("SELECT vector FROM event_vectors WHERE event_id = ? AND model_key = ?",
                                  (key, model_key))
    else:
        rows = connection.execute(
            """SELECT v.vector FROM chunk_vectors v JOIN document_chunks c ON c.id = v.chunk_id
            WHERE c.document_id = ? AND v.model_key = ? ORDER BY c.ordinal""", (key, model_key))
    total: list[float] | None = None
    for (blob,) in rows:
        values = _unpack(blob)
        if total is None:
            total = values
        elif len(values) == len(total):
            total = [a + b for a, b in zip(total, values)]
    norm = math.sqrt(_dot(total, total)) if total else 0.0
    return [value / norm for value in total] if total and norm else None


def memory_similarity(connection, model_key: str, a: str, b: str) -> float | None:
    """How alike two memories are by meaning, as the map compares them; None without vectors to compare."""
    first, second = _memory_vector(connection, model_key, a), _memory_vector(connection, model_key, b)
    if first is None or second is None or len(first) != len(second):
        return None
    return _dot(first, second)


def _standing(keys: list[str], moved: set[str], old_lists: dict[str, list[tuple[float, str]]],
              invalid: set[str]) -> tuple[Neighbours, list[str]]:
    """The neighbour lists that still hold, as heaps, and the memories whose list must be computed again."""
    lists: Neighbours = {}
    again: list[str] = []
    for key in keys:
        before = [] if key in moved else old_lists.get(key, [])
        kept = [(similarity, other) for similarity, other in before if other not in invalid]
        if len(kept) < len(before):
            again.append(key)
            kept = []
        heapq.heapify(kept)
        lists[key] = kept
    return lists, again


def nearest_neighbours(vectors: dict[str, list[float]], checksums: dict[str, int],
                       previous: dict[str, Any] | None = None, *, k: int = SIMILAR_K,
                       minimum: float = SIMILAR_MIN) -> tuple[Neighbours, dict[str, Any]]:
    """Each memory's *k* nearest by meaning, at *minimum* similarity or more, and what the pass cost.

    A fresh pass compares every pair once (about 4 µs per pair at 1024
    dimensions: 40 s for 3 800 memories). With *previous*, the neighbours and
    checksums of an earlier pass, only the memories that are new or changed
    are compared with all the others, and a memory that lost a neighbour to a
    change or a removal is compared again; every other list stands. The
    result is the same as a fresh pass.
    """
    keys = sorted(vectors)
    old_sums: dict[str, int] = previous["checksums"] if previous else {}
    old_lists: dict[str, list[tuple[float, str]]] = previous["neighbours"] if previous else {}
    changed = [key for key in keys if old_sums.get(key) != checksums[key]]
    mode = "incremental"
    if previous is None or len(changed) > INCREMENTAL_SHARE * len(keys):
        changed, mode = keys, "full"
    moved = set(changed)
    lists, again = _standing(keys, moved, old_lists, moved | (old_sums.keys() - vectors.keys()))
    if mode == "incremental" and (len(changed) + len(again)) * len(keys) >= len(keys) * (len(keys) - 1) // 2:
        changed, mode = keys, "full"  # the update would cost more than a fresh pass
        moved = set(changed)
        lists, again = {key: [] for key in keys}, []

    def offer(key: str, other: str, similarity: float) -> None:
        # (similarity, id) is a total order: equal resemblances (copies score 1.0)
        # keep the same neighbours whatever order the memories were compared in.
        heap, entry = lists[key], (similarity, other)
        if len(heap) < k:
            heapq.heappush(heap, entry)
        elif entry > heap[0]:
            heapq.heapreplace(heap, entry)

    order = changed + [key for key in keys if key not in moved]
    rows = [vectors[key] for key in order]
    for key in again:  # its whole list again, against every memory as it is now
        row = vectors[key]
        for other, similarity in zip(order, map(_dot, repeat(row), rows)):
            if similarity >= minimum and other != key:
                offer(key, other, similarity)
    redone = set(again)
    for i, key in enumerate(changed):  # the changed ones come first in order: each pair once
        for offset, similarity in enumerate(map(_dot, repeat(rows[i]), rows[i + 1:])):
            if similarity < minimum:
                continue
            other = order[i + 1 + offset]
            offer(key, other, similarity)
            if other not in redone:
                offer(other, key, similarity)
    compared = len(changed) * len(keys) - len(changed) * (len(changed) + 1) // 2 + len(again) * len(keys)
    return lists, {"mode": mode, "memories": len(keys), "changed": len(changed), "rechecked": len(again),
                   "compared": compared}


def neighbour_edges(neighbours: Neighbours) -> list[list[Any]]:
    """The unique pairs of the neighbour lists: ``[id, id, similarity, duplicate]``."""
    pairs: dict[tuple[str, str], float] = {}
    for key, heap in neighbours.items():
        for similarity, other in heap:
            pairs[(min(key, other), max(key, other))] = similarity
    return [[a, b, round(similarity, 3), 1 if similarity >= DUPLICATE_MIN else 0]
            for (a, b), similarity in sorted(pairs.items())]


def load_similar(path: Path) -> dict[str, Any] | None:
    """The cached pass at *path*, or None when there is none or it was written by another version."""
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(cached, dict) or cached.get("version") != SIMILAR_VERSION:
        return None
    if cached.get("k") != SIMILAR_K or cached.get("minimum") != SIMILAR_MIN:
        return None
    if not isinstance(cached.get("model"), str) or not isinstance(cached.get("signature"), list):
        return None
    try:
        cached["neighbours"] = {str(key): [(float(s), str(o)) for s, o in heap]
                                for key, heap in cached["neighbours"].items()}
        cached["checksums"] = {str(key): int(value) for key, value in cached["checksums"].items()}
    except (AttributeError, KeyError, TypeError, ValueError):  # a damaged file is no cache
        return None
    return cached


def refresh_similar(manifest: dict[str, Any], *, full: bool = False) -> dict[str, Any]:
    """Bring the cached edges by meaning up to date with the stored vectors; what it did."""
    config = config_from_manifest(manifest)
    model_key = active_profile().key
    path = similar_cache_path(config)
    previous = None if full else load_similar(path)
    if previous is not None and previous.get("model") != model_key:
        previous = None
    with _read_only(config) as connection:
        connection.execute("BEGIN")  # one snapshot: the signature describes exactly the vectors read
        signature = vector_digest(connection, model_key)[0]
        if previous is not None and previous["signature"] == signature:
            return {"status": "fresh", "model": model_key, "edges": len(neighbour_edges(previous["neighbours"]))}
        vectors, checksums = memory_vectors(connection, model_key)
        connection.rollback()
    started = time.monotonic()
    neighbours, stats = nearest_neighbours(vectors, checksums, previous)
    seconds = round(time.monotonic() - started, 2)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    partial.write_text(json.dumps({
        "version": SIMILAR_VERSION, "model": model_key, "k": SIMILAR_K, "minimum": SIMILAR_MIN,
        "signature": signature, "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seconds": seconds, **stats, "checksums": checksums,
        "neighbours": {key: [[s, o] for s, o in sorted(heap, reverse=True)] for key, heap in neighbours.items() if heap},
    }, separators=(",", ":")), encoding="utf-8")
    os.replace(partial, path)
    return {"status": "computed", "model": model_key, "seconds": seconds, **stats,
            "edges": len(neighbour_edges(neighbours)), "path": str(path)}


class SimilarRefresher:
    """Keeps the edges by meaning current for the server: one pass at a time, in a process of its own.

    A pass reads every vector into memory and runs for up to a minute of CPU;
    in the server it would hold the interpreter from the tools and keep its
    memory. A pass that failed is not started again for ``SIMILAR_RETRY_S``.
    """

    def __init__(self, manifest_path: Path, *, timeout_s: float = SIMILAR_TIMEOUT_S,
                 retry_s: float = SIMILAR_RETRY_S) -> None:
        self._manifest_path = manifest_path
        self._timeout_s = timeout_s
        self._retry_s = retry_s
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self._failed_at: float | None = None
        self._loaded: tuple[tuple[int, int], dict[str, Any], list[list[Any]]] | None = None

    def cached(self, path: Path) -> tuple[dict[str, Any], list[list[Any]]] | None:
        """The cached pass and its edges, read again only when the file changed."""
        try:
            stat = path.stat()
        except OSError:
            return None
        stamp = (stat.st_mtime_ns, stat.st_size)
        with self._lock:
            if self._loaded is not None and self._loaded[0] == stamp:
                return self._loaded[1], self._loaded[2]
        cached = load_similar(path)
        if cached is None:
            return None
        edges = neighbour_edges(cached.pop("neighbours"))
        cached.pop("checksums", None)
        with self._lock:
            self._loaded = (stamp, cached, edges)
        return cached, edges

    def ensure(self, log_path: Path) -> str:
        """Start a pass unless one runs or the last one failed lately: ``running`` or ``failed``."""
        with self._lock:
            if self._process is not None:
                return "running"
            if self._failed_at is not None and time.monotonic() - self._failed_at < self._retry_s:
                return "failed"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if log_path.exists() and log_path.stat().st_size > SIMILAR_LOG_BYTES:
                log_path.replace(log_path.with_name(log_path.name + ".1"))  # the one before stays, older ones go
            with open(log_path, "ab") as log:
                log.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} atlas similar\n".encode())
                log.flush()
                self._process = subprocess.Popen(
                    [sys.executable, "-P", "-m", "max_chronicle.cli", "--manifest", str(self._manifest_path),
                     "atlas", "similar"],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                    env=_same_code_env())
            threading.Thread(target=self._watch, args=(self._process,), name="chronicle-atlas-similar",
                             daemon=True).start()
            return "running"

    def _watch(self, process: subprocess.Popen[bytes]) -> None:
        try:
            code = process.wait(timeout=self._timeout_s)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            code = -1
        with self._lock:
            self._process = None
            self._failed_at = None if code == 0 else time.monotonic()


def _same_code_env() -> dict[str, str]:
    """The environment of a pass that runs this very code.

    Installed, the package is where the interpreter finds it anyway. Run from
    a source checkout, an installed copy (perhaps an older release) would
    shadow it, so the checkout comes first on the path.
    """
    env = os.environ.copy()
    root = str(Path(__file__).resolve().parents[1])
    installed = {str(Path(path).resolve()) for path in [*site.getsitepackages(), site.getusersitepackages()]}
    if root not in installed:
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [root, env.get("PYTHONPATH")]))
    return env


def similar_payload(manifest: dict[str, Any], refresher: SimilarRefresher) -> dict[str, Any]:
    """The edges by meaning for the page, and whether they are current.

    ``ready``: current. ``stale``: from an earlier pass while a new one runs.
    ``computing``: the first pass runs. ``none``: no vectors to compare.
    ``failed``: the last pass failed (its log says why).
    """
    config = config_from_manifest(manifest)
    model_key = active_profile().key
    with _read_only(config) as connection:
        signature, shown = vector_digest(connection, model_key)
    path = similar_cache_path(config)
    found = refresher.cached(path)
    if found is not None and found[0]["model"] == model_key and found[0]["signature"] == signature:
        return {"status": "ready", "edges": found[1], "computed_at": found[0].get("computed_at")}
    if not shown:
        return {"status": "none", "edges": []}
    state = refresher.ensure(path.with_suffix(".log"))
    if found is not None and found[0]["model"] == model_key:
        # An earlier pass knows memories hidden or deleted since: they are left out.
        edges = [edge for edge in found[1] if edge[0] in shown and edge[1] in shown]
        return {"status": "stale" if state == "running" else state, "edges": edges,
                "computed_at": found[0].get("computed_at")}
    return {"status": "computing" if state == "running" else state, "edges": []}


# ---------------------------------------------------------------------------
# The page: its files, its policy, its data
# ---------------------------------------------------------------------------

UI_DIR = Path(__file__).resolve().parent / "ui" / "atlas"
_STATIC_NAME = re.compile(r"[a-z][a-z0-9.-]*\.(js|css)")
_STATIC_TYPES = {"js": "text/javascript; charset=utf-8", "css": "text/css; charset=utf-8"}
# The page runs its own scripts, a layout worker and requests to this server, nothing else.
CSP = ("default-src 'none'; script-src 'self'; worker-src 'self'; connect-src 'self'; style-src 'self'; "
       "img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
SECURITY_HEADERS = {
    "Content-Security-Policy": CSP,
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cache-Control": "no-store",
}


def page() -> bytes:
    return (UI_DIR / "index.html").read_bytes()


def static_file(name: str) -> tuple[bytes, str] | None:
    """One of the page's scripts or styles by its bare file name, and its type; None for anything else."""
    match = _STATIC_NAME.fullmatch(name)
    if match is None:
        return None
    path = UI_DIR / name
    if not path.is_file():
        return None
    return path.read_bytes(), _STATIC_TYPES[match[1]]


def graph_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    with _read_only(config) as connection:
        connection.execute("BEGIN")
        graph = build_graph(connection, config.identities)
        connection.rollback()
    return graph


def _copy_label(connection, target: str) -> str | None:
    """A hidden copy's label; None when it is gone, or hidden since for another reason than being a copy."""
    kind, _, key = target.partition(":")
    if kind == "event":
        row = connection.execute(
            "SELECT text FROM events WHERE id = ? AND visibility = 'raw_only' "
            "AND json_extract(payload_json, '$.memory_guard.source') = ?", (key, curation.GUARD_SOURCE)).fetchone()
    else:
        row = connection.execute("SELECT title FROM documents WHERE id = ? AND deleted_at_utc IS NULL",
                                 (key,)).fetchone()
    return None if row is None else _label(row[0])


def item_payload(manifest: dict[str, Any], identifier: str) -> dict[str, Any] | None:
    """One event or note in full for the side panel, redacted again on the way out; None when hidden or unknown.

    ``copies`` are the memories hidden as copies of it, to restore.
    """
    kind, _, key = identifier.partition(":")
    if kind not in ("event", "note"):
        raise ValueError("id must be event:<id> or note:<id>")
    with _read_only(config_from_manifest(manifest)) as connection:
        if kind == "event" and connection.execute(
                f"SELECT 1 FROM events e WHERE e.id = ? AND {_SHOWN_EVENT}", (key,)).fetchone() is None:
            return None
        copies = [{"id": copy, "label": label, **mark.public()}
                  for copy, mark in curation.marks(connection).copies_of(identifier)
                  if (label := _copy_label(connection, copy)) is not None]
    found = get_items(manifest, [identifier])["items"]
    if not found:
        return None
    return redact_value(found[0], skip_keys=_ITEM_IDENTIFIERS)[0] | {"copies": copies}
