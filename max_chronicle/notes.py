"""An index of notes written on purpose (FR-10).

Agents keep durable notes as files, such as Claude's file memory
(``~/.claude/projects/<directory>/memory/*.md``). Chronicle indexes them so
every agent can recall them. The files stay the source: the index is rebuilt
from them and never writes to them. A changed note is re-indexed, a deleted one
keeps a tombstone, and the secret filter runs on every stored string (FR-11).

A note inside a project root, or in the file memory of a project's directory,
belongs to that project; any other note is global and ranks below project
notes in a project-scoped query. Notes have no task or domain.

The manifest turns indexing on:

    [notes]
    paths = ["~/.claude/projects/*/memory/*.md"]
    exclude = ["*.bak-*", "*/_archive/*"]
    deny = ["*api-key*", "*keys.md"]   # never read, whatever they hold
    sync_minutes = 15                   # the server re-syncs this often
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
import glob
import hashlib
import os
from pathlib import Path
import re
import sqlite3
from typing import Any

from . import embeddings
from .embeddings import BACKFILL_TIMEOUT_S, active_profile, pack_vector
from .identity import Registry
from .lexical import covers, query_terms
from .redaction import redact
from .store import _fts_query, config_from_manifest, open_connection, write_transaction

NOTE_SOURCE = "notes"
# Notes that hold keys are skipped by name as well as filtered by content.
DEFAULT_DENY = ("*api-key*", "*api_key*", "*apikey*", "*secret*", "*credential*", "*password*", "*keys.md")
CHUNK_CHARS = 1500  # a longer section is split at paragraphs
MAX_NOTE_BYTES = 512 * 1024  # a larger file is not a note
EMBED_BATCH = 16
NOTE_TEXT_CHARS = 800  # of a chunk, in a recall hit; the resource serves the whole note

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")


@dataclass(frozen=True)
class NoteSettings:
    paths: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    deny: tuple[str, ...] = DEFAULT_DENY
    sync_minutes: int | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.paths)


def note_settings(manifest: dict[str, Any]) -> NoteSettings:
    """The [notes] section of *manifest*; without it, nothing is indexed."""
    raw = manifest.get("notes") or {}

    def names(key: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
        value = raw.get(key, default)
        if isinstance(value, str) or not isinstance(value, (list, tuple)):
            raise ValueError(f"[notes] {key} must be a list of patterns")
        return tuple(str(item) for item in value if str(item).strip())

    minutes = raw.get("sync_minutes")
    if minutes is not None and (isinstance(minutes, bool) or not isinstance(minutes, int) or minutes < 1):
        raise ValueError("[notes] sync_minutes must be a whole number of minutes, at least 1")
    return NoteSettings(names("paths"), names("exclude"), names("deny", DEFAULT_DENY) + DEFAULT_DENY, minutes)


def _utc(timestamp: float | None = None) -> str:
    moment = datetime.fromtimestamp(timestamp, timezone.utc) if timestamp is not None else datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def document_id(path: Path) -> str:
    return hashlib.sha256(str(path).encode()).hexdigest()[:32]


def discover(settings: NoteSettings) -> list[Path]:
    """Every note file the settings name, excluded ones left out."""
    found: set[Path] = set()
    for pattern in settings.paths:
        for name in glob.glob(os.path.expanduser(pattern), recursive=True):
            path = Path(name)
            if path.is_file() and not any(
                fnmatch(str(path), rule) or fnmatch(path.name, rule) for rule in settings.exclude
            ):
                found.add(path)
    return sorted(found)


def denied(path: Path, settings: NoteSettings) -> bool:
    name = path.name.lower()
    return any(fnmatch(name, rule.lower()) for rule in settings.deny)


def _directory_slug(path: str | Path) -> str:
    """The folder name Claude Code gives a working directory's file memory."""
    return re.sub(r"[^A-Za-z0-9]", "-", os.path.normpath(os.path.expanduser(str(path))))


def note_project(path: Path, manifest: dict[str, Any], identities: Registry) -> str | None:
    """The canonical project a note belongs to, or None for a global note."""
    where = Path(os.path.normpath(path))
    best: tuple[int, str] | None = None
    memory_folder = where.parent.parent.name if where.parent.name == "memory" else None
    for entry in manifest.get("projects", []):
        for root in entry.get("roots", []):
            root_path = Path(os.path.normpath(os.path.expanduser(root)))
            if root_path in where.parents or memory_folder == _directory_slug(root_path):
                if best is None or len(root_path.parts) > best[0]:
                    best = (len(root_path.parts), entry["id"])
    if best is not None:
        return identities.project(best[1])
    workspace = (manifest.get("paths") or {}).get("workspace_root")
    if memory_folder and workspace:
        prefix = _directory_slug(workspace)
        if memory_folder.startswith(prefix + "-"):
            return identities.project(memory_folder[len(prefix) + 1:])
    return None


def _frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Top-level ``key: value`` pairs of a leading --- block (nested keys flattened), and the body."""
    if not text.startswith("---"):
        return {}, text
    lines = text.splitlines()
    try:
        end = next(index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---")
    except StopIteration:
        return {}, text
    fields: dict[str, str] = {}
    for line in lines[1:end]:
        key, sep, value = line.strip().partition(":")
        if sep and key and value.strip():
            fields.setdefault(key.strip(), value.strip().strip("\"'"))
    return fields, "\n".join(lines[end + 1:])


def _split(text: str, limit: int = CHUNK_CHARS) -> list[str]:
    """*text* in pieces of at most *limit* characters, cut at paragraphs, then lines."""
    if len(text) <= limit:
        return [text]
    pieces: list[str] = []
    current = ""
    for part in re.split(r"(\n\s*\n)", text):
        if len(current) + len(part) <= limit:
            current += part
            continue
        if current.strip():
            pieces.append(current)
        current = part
        while len(current) > limit:
            cut = current.rfind("\n", 0, limit)
            cut = cut if cut > limit // 2 else limit
            pieces.append(current[:cut])
            current = current[cut:]
    if current.strip():
        pieces.append(current)
    return [piece.strip("\n") for piece in pieces]


def chunk_note(title: str, body: str) -> list[tuple[str, str]]:
    """(heading, text) sections of a note; a heading is ``Title › Section``."""
    sections: list[tuple[str, list[str]]] = [(title, [])]
    fenced = False
    for line in body.splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        match = None if fenced else _HEADING.match(line)
        if match and len(match.group(1)) > 1:
            sections.append((f"{title} › {match.group(2).strip()}", []))
        elif not (match and match.group(2).strip() == title):
            sections[-1][1].append(line)
    chunks = []
    for heading, lines in sections:
        text = "\n".join(lines).strip()
        if text:
            chunks.extend((heading, piece) for piece in _split(text))
    return chunks


@dataclass
class ParsedNote:
    path: Path
    sha256: str
    size: int
    modified: str
    project: str | None
    title: str
    description: str | None
    kind: str | None
    chunks: list[tuple[str, str]]
    redactions: int


def parse_note(path: Path, data: bytes, project: str | None) -> ParsedNote:
    """A note's fields and chunks, with likely secrets redacted in every stored string."""
    text = data.decode("utf-8", errors="replace")
    fields, body = _frontmatter(text)
    heading = next((match.group(2).strip() for line in body.splitlines()
                    if (match := _HEADING.match(line)) and len(match.group(1)) == 1), None)
    redactions = 0

    def clean(value: str | None) -> str | None:
        nonlocal redactions
        if value is None:
            return None
        result = redact(value)
        redactions += result.count
        return result.text

    title = clean(fields.get("name") or heading or path.stem) or path.stem
    chunks = [(clean(head) or title, clean(chunk) or "") for head, chunk in chunk_note(title, body)]
    return ParsedNote(
        path=path,
        sha256=hashlib.sha256(data).hexdigest(),
        size=len(data),
        modified=_utc(path.stat().st_mtime),
        project=project,
        title=title,
        description=clean(fields.get("description")),
        kind=fields.get("type"),
        chunks=chunks,
        redactions=redactions,
    )


def _store(connection: sqlite3.Connection, note: ParsedNote, now: str) -> None:
    identifier = document_id(note.path)
    connection.execute(
        """INSERT INTO documents(id, path, source, project, title, description, kind, content_sha256,
               size_bytes, modified_at_utc, indexed_at_utc, deleted_at_utc, redactions)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
           ON CONFLICT(id) DO UPDATE SET project=excluded.project, title=excluded.title,
               description=excluded.description, kind=excluded.kind, content_sha256=excluded.content_sha256,
               size_bytes=excluded.size_bytes, modified_at_utc=excluded.modified_at_utc,
               indexed_at_utc=excluded.indexed_at_utc, deleted_at_utc=NULL, redactions=excluded.redactions""",
        (identifier, str(note.path), NOTE_SOURCE, note.project, note.title, note.description, note.kind,
         note.sha256, note.size, note.modified, now, note.redactions),
    )
    connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (identifier,))
    connection.executemany(
        "INSERT INTO document_chunks(id, document_id, ordinal, heading, text) VALUES (?, ?, ?, ?, ?)",
        [(f"{identifier}:{ordinal}", identifier, ordinal, heading, text)
         for ordinal, (heading, text) in enumerate(note.chunks)],
    )


def sync_notes(manifest: dict[str, Any], *, embed: bool = True, embed_limit: int | None = None) -> dict[str, Any]:
    """Bring the index in line with the note files; the files are only read."""
    settings = note_settings(manifest)
    config = config_from_manifest(manifest)
    report: dict[str, Any] = {"enabled": settings.enabled, "indexed": 0, "unchanged": 0, "removed": 0,
                              "denied": 0, "skipped": [], "redactions": 0}
    if not settings.enabled:
        return report
    identities = config.identities
    with open_connection(config) as connection:
        known = {row["path"]: (row["content_sha256"], row["project"], row["deleted_at_utc"])
                 for row in connection.execute("SELECT path, content_sha256, project, deleted_at_utc FROM documents "
                                               "WHERE source = ?", (NOTE_SOURCE,))}
    live: set[str] = set()
    changed: list[ParsedNote] = []
    for path in discover(settings):
        if denied(path, settings):
            report["denied"] += 1
            continue
        try:
            if path.stat().st_size > MAX_NOTE_BYTES:
                report["skipped"].append({"path": str(path), "reason": "too_large"})
                continue
            data = path.read_bytes()
        except OSError as exc:
            report["skipped"].append({"path": str(path), "reason": type(exc).__name__})
            continue
        live.add(str(path))
        project = note_project(path, manifest, identities)
        digest = hashlib.sha256(data).hexdigest()
        if known.get(str(path)) == (digest, project, None):
            report["unchanged"] += 1
            continue
        note = parse_note(path, data, project)
        changed.append(note)
        report["redactions"] += note.redactions
    now = _utc()
    with write_transaction(config) as connection:
        for note in changed:
            _store(connection, note, now)
            report["indexed"] += 1
        for path, (_, _, deleted) in known.items():
            if path not in live and deleted is None:
                identifier = document_id(Path(path))
                connection.execute("UPDATE documents SET deleted_at_utc = ? WHERE id = ?", (now, identifier))
                connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (identifier,))
                report["removed"] += 1
    if embed:
        report["embedding"] = embed_chunks(manifest, limit=embed_limit)
    return report


def embed_chunks(manifest: dict[str, Any], *, limit: int | None = None) -> dict[str, Any]:
    """Embed live chunks that have no vector for the active model; the index works without them."""
    config = config_from_manifest(manifest)
    profile = active_profile()
    with open_connection(config) as connection:
        rows = connection.execute(
            """SELECT c.id, c.heading, c.text FROM document_chunks c
               JOIN documents d ON d.id = c.document_id AND d.deleted_at_utc IS NULL
               LEFT JOIN chunk_vectors v ON v.chunk_id = c.id AND v.model_key = ?
               WHERE v.chunk_id IS NULL ORDER BY d.modified_at_utc DESC, c.id"""
            + (" LIMIT ?" if limit else ""),
            (profile.key, limit) if limit else (profile.key,),
        ).fetchall()
    embedded = failed = 0
    for start in range(0, len(rows), EMBED_BATCH):
        batch = rows[start:start + EMBED_BATCH]
        vectors = embeddings.embed_documents([f"{row['heading']}\n{row['text']}" for row in batch],
                                             timeout=BACKFILL_TIMEOUT_S)
        if vectors is None:
            failed += len(rows) - start  # the backend is down: stop, the next sync retries
            break
        with write_transaction(config) as connection:
            connection.executemany(
                """INSERT INTO chunk_vectors(chunk_id, model_key, dim, vector, created_at_utc)
                   SELECT ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM document_chunks WHERE id = ?)
                   ON CONFLICT(chunk_id, model_key) DO NOTHING""",
                [(row["id"], profile.key, len(vector), pack_vector(vector), _utc(), row["id"])
                 for row, vector in zip(batch, vectors)],
            )
        embedded += len(batch)
    return {"model_key": profile.key, "missing": len(rows), "embedded": embedded, "failed": failed}


def notes_status(manifest: dict[str, Any]) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    profile = active_profile()
    with open_connection(config) as connection:
        live, deleted, projects = connection.execute(
            "SELECT count(*) FILTER (WHERE deleted_at_utc IS NULL), count(*) FILTER (WHERE deleted_at_utc IS NOT NULL), "
            "count(DISTINCT project) FILTER (WHERE deleted_at_utc IS NULL) FROM documents").fetchone()
        chunks, vectors = connection.execute(
            """SELECT count(*), count(v.chunk_id) FROM document_chunks c
               LEFT JOIN chunk_vectors v ON v.chunk_id = c.id AND v.model_key = ?""", (profile.key,)).fetchone()
        synced = connection.execute("SELECT max(indexed_at_utc) FROM documents").fetchone()[0]
    return {"enabled": note_settings(manifest).enabled, "notes": live, "tombstones": deleted, "projects": projects,
            "chunks": chunks, "vectors": {"model_key": profile.key, "chunks_with_vector": vectors},
            "last_indexed_at_utc": synced}


def read_note(manifest: dict[str, Any], identifier: str) -> dict[str, Any] | None:
    """A note's redacted text and fields, as indexed."""
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        row = connection.execute("SELECT * FROM documents WHERE id = ?", (identifier,)).fetchone()
        if row is None:
            return None
        chunks = connection.execute(
            "SELECT heading, text FROM document_chunks WHERE document_id = ? ORDER BY ordinal", (identifier,)).fetchall()
    text, heading = [], None
    for chunk in chunks:
        if chunk["heading"] != heading:
            heading = chunk["heading"]
            text.append(f"## {heading}")
        text.append(chunk["text"])
    return {key: row[key] for key in ("id", "path", "project", "title", "description", "kind",
                                      "modified_at_utc", "indexed_at_utc", "deleted_at_utc", "redactions")} | {
        "text": "\n\n".join(text)}


def recall_notes(
    config,
    *,
    query: str,
    project: str | None,
    query_vector: list[float] | None,
    threshold: float,
    limit: int,
) -> dict[str, Any]:
    """Notes matching *query*, best chunk per note; in a project scope, project notes first (FR-10)."""
    from .embeddings import similarity_scorer, unpack_vector  # noqa: PLC0415 - read at call time

    profile = active_profile()
    scope = config.identities.scope(project=project)
    where, params = scope.where(domain="NULL", project="d.project", task="NULL", project_or_unset=True)
    with open_connection(config) as connection:
        if not _has_documents(connection):
            return {"notes": [], "strict": set(), "vector_available": False}
        pool = {row["id"]: dict(row) for row in connection.execute(
            """SELECT c.id, c.document_id, c.heading, c.text, d.path, d.title, d.project, d.kind, d.modified_at_utc,
                      v.dim, v.vector
               FROM document_chunks c JOIN documents d ON d.id = c.document_id AND d.deleted_at_utc IS NULL
               LEFT JOIN chunk_vectors v ON v.chunk_id = c.id AND v.model_key = ?
               WHERE """ + where, (profile.key, *params))}
        if not pool:
            return {"notes": [], "strict": set(), "vector_available": False}
        terms = query_terms(query)
        strict = {row[0] for row in connection.execute(
            "SELECT chunk_id FROM document_chunks_fts WHERE document_chunks_fts MATCH ? ORDER BY bm25(document_chunks_fts)",
            (_fts_query(query),))} & set(pool) if terms else set()
        fts: list[str] = []
        if terms:
            relaxed = not strict
            for row in connection.execute(
                "SELECT chunk_id FROM document_chunks_fts WHERE document_chunks_fts MATCH ? "
                "ORDER BY bm25(document_chunks_fts) LIMIT ?",
                (_fts_query(query, relaxed=relaxed), max(limit * 20, 100)),
            ):
                chunk = pool.get(row[0])
                if chunk and (not relaxed or covers(terms, f"{chunk['heading']} {chunk['text']}")):
                    fts.append(row[0])
    fts_ranks = {chunk_id: rank for rank, chunk_id in enumerate(fts)}
    similarities: dict[str, float] = {}
    vector_available = False
    if query_vector is not None:
        score = similarity_scorer(query_vector)
        for chunk_id, chunk in pool.items():
            if chunk["vector"] is not None and chunk["dim"] == len(query_vector):
                vector_available = True
                similarities[chunk_id] = score(unpack_vector(chunk["vector"]))
    admitted = sorted((cid for cid, value in similarities.items() if value >= threshold),
                      key=lambda cid: -similarities[cid])[: max(limit * 20, 100)]
    vector_ranks = {chunk_id: rank for rank, chunk_id in enumerate(admitted)}
    scores = {chunk_id: sum(1 / (60 + rank) for rank in (fts_ranks.get(chunk_id), vector_ranks.get(chunk_id))
                            if rank is not None)
              for chunk_id in set(fts_ranks) | set(vector_ranks)}
    best: dict[str, str] = {}
    for chunk_id in sorted(scores, key=lambda cid: (-scores[cid], cid)):
        best.setdefault(pool[chunk_id]["document_id"], chunk_id)
    ordered = sorted(best.values(), key=lambda cid: (
        scope.project is not None and pool[cid]["project"] is None,  # global notes after project notes
        -scores[cid], pool[cid]["modified_at_utc"], cid))[:limit]
    notes = [{
        "document_id": pool[cid]["document_id"],
        "chunk_id": cid,
        "title": pool[cid]["title"],
        "heading": pool[cid]["heading"],
        "text": pool[cid]["text"][:NOTE_TEXT_CHARS] + ("…" if len(pool[cid]["text"]) > NOTE_TEXT_CHARS else ""),
        "project": pool[cid]["project"],
        "kind": pool[cid]["kind"],
        "path": pool[cid]["path"],
        "modified_at_utc": pool[cid]["modified_at_utc"],
        "rrf_score": round(scores[cid], 6),
        "channels": {"fts_rank": fts_ranks.get(cid), "vector_similarity": similarities.get(cid)},
    } for cid in ordered]
    return {"notes": notes, "strict": strict, "similarities": similarities, "vector_available": vector_available}


def _has_documents(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'documents'").fetchone() is not None
