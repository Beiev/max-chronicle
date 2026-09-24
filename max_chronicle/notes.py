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
    paths = ["~/.claude/projects/*/memory/*.md"]   # absolute, or from ~
    exclude = ["*.bak-*", "*/_archive/*"]
    deny = ["*api-key*", "*keys.md"]   # never read, whatever they hold
    sync_minutes = 15                   # the server re-syncs this often

Symbolic links below the fixed part of a path pattern are not followed, and a
file that is not text, or whose path holds a likely secret, is not read.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatch
import glob
import hashlib
import itertools
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
# A note about a key (which file, which host) stays; a pasted key block is redacted by content.
DEFAULT_DENY = ("*api-key*", "*api_key*", "*apikey*", "*secret*", "*credential*", "*password*", "*keys.md",
                "*private-key*", "*private_key*", "id_rsa*", "id_ed25519*", "id_ecdsa*", "*.pem")
CHUNK_CHARS = 1500  # a longer section is split at paragraphs
MAX_NOTE_BYTES = 512 * 1024  # a larger file is not a note
EMBED_BATCH = 16
NOTE_TEXT_CHARS = 800  # of a chunk, in a recall hit; the resource serves the whole note
RELAXED_SCAN_PAGES = 4  # a relaxed full-text query reads at most this many candidate lists
WALK_BUDGET = 20_000  # directory entries read to find the directory of a file-memory folder

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")
_GLOB_CHARS = re.compile(r"[*?\[]")
# A frontmatter line: `key: value`, `key:`, an indented continuation, a list item or a comment.
_FRONTMATTER_LINE = re.compile(r"^(?:[A-Za-z0-9_][\w.-]*\s*:(?:\s|$)|\s+\S|-\s|#)")
# Longer marks first: the UTF-32 LE mark starts with the UTF-16 LE one.
_BOMS = ((codecs.BOM_UTF32_LE, "utf-32"), (codecs.BOM_UTF32_BE, "utf-32"), (codecs.BOM_UTF8, "utf-8-sig"),
         (codecs.BOM_UTF16_LE, "utf-16"), (codecs.BOM_UTF16_BE, "utf-16"))


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
    paths = names("paths")
    # A relative pattern would follow the current directory of whichever process syncs.
    relative = [pattern for pattern in paths if not os.path.isabs(os.path.expanduser(pattern))]
    if relative:
        raise ValueError(f"[notes] paths must be absolute or start with ~: {', '.join(relative)}")
    return NoteSettings(paths, names("exclude"), names("deny", DEFAULT_DENY) + DEFAULT_DENY, minutes)


def _utc(timestamp: float | None = None) -> str:
    moment = datetime.fromtimestamp(timestamp, timezone.utc) if timestamp is not None else datetime.now(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def document_id(path: Path) -> str:
    return hashlib.sha256(str(path).encode()).hexdigest()[:32]


def _glob_base(pattern: str) -> Path:
    """The fixed directory of a path pattern: its parts before the first wildcard, file name excluded."""
    parts = Path(pattern).parts
    fixed = list(itertools.takewhile(lambda part: not _GLOB_CHARS.search(part), parts[:-1]))
    return Path(*fixed) if fixed else Path(pattern).parent


def _through_symlink(path: Path, base: Path) -> bool:
    """Whether *path* is reached from *base* through a symbolic link; *base* itself may be one."""
    try:
        return path.resolve() != base.resolve() / path.relative_to(base)
    except (OSError, ValueError):
        return True


def discover(settings: NoteSettings) -> tuple[list[Path], list[Path]]:
    """The note files the settings name, excluded ones left out, and those reached through a symlink.

    A link could lead past the deny list, or out of the configured paths, so
    such files are reported, never read.
    """
    plain: dict[Path, bool] = {}
    for pattern in settings.paths:
        expanded = os.path.normpath(os.path.expanduser(pattern))
        base = _glob_base(expanded)
        for name in glob.glob(expanded, recursive=True):
            path = Path(name)
            if not path.is_file() or any(fnmatch(str(path), rule) or fnmatch(path.name, rule)
                                         for rule in settings.exclude):
                continue
            plain[path] = plain.get(path, False) or not _through_symlink(path, base)
    return sorted(path for path, ok in plain.items() if ok), sorted(path for path, ok in plain.items() if not ok)


def denied(path: Path, settings: NoteSettings) -> bool:
    name = path.name.lower()
    return any(fnmatch(name, rule.lower()) for rule in settings.deny)


def _encode(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "-", name)


def _directory_slug(path: str | Path) -> str:
    """The folder name Claude Code gives a working directory's file memory."""
    return _encode(os.path.normpath(os.path.expanduser(str(path))))


def _slug_directory(slug: str) -> Path | None:
    """The existing directory whose file-memory folder is *slug*, found from the filesystem root.

    The folder name alone is ambiguous (`a/b` and `a-b` give the same one), so
    the directories on disk decide.
    """
    budget = WALK_BUDGET

    def walk(directory: Path, rest: str) -> Path | None:
        nonlocal budget
        if not rest:
            return directory
        try:
            with os.scandir(directory) as listing:
                entries = sorted(listing, key=lambda entry: entry.name)
        except OSError:
            return None
        for entry in entries:
            budget -= 1
            if budget < 0:
                return None
            encoded = "-" + _encode(entry.name)
            if (rest == encoded or rest.startswith(encoded + "-")) and entry.is_dir():
                found = walk(Path(entry.path), rest[len(encoded):])
                if found is not None:
                    return found
        return None

    return walk(Path(os.path.abspath(os.sep)), slug) if slug.startswith("-") else None


def note_project(
    path: Path,
    manifest: dict[str, Any],
    identities: Registry,
    directories: dict[str, Path | None] | None = None,
) -> str | None:
    """The canonical project a note belongs to, or None for a global note.

    A note inside a project root belongs to it. A note in the file memory of a
    working directory belongs to the project of that directory: the project
    whose root holds it, else the workspace child it sits in. When the
    directory is gone, the folder name decides: under a root's name the note
    stays with the root, and a workspace child keeps its name. *directories*
    caches folder lookups across the notes of one sync.
    """
    where = Path(os.path.normpath(path))
    roots = [(Path(os.path.normpath(os.path.expanduser(root))), entry["id"])
             for entry in manifest.get("projects", []) for root in entry.get("roots", [])]

    def enclosing(directory: Path) -> str | None:
        best = max(((len(root.parts), project) for root, project in roots
                    if root == directory or root in directory.parents), default=None)
        return identities.project(best[1]) if best else None

    folder = where.parent.parent.name if where.parent.name == "memory" else None
    if (project := enclosing(where.parent)) or not folder:
        return project
    exact = [project for root, project in roots if _directory_slug(root) == folder]
    if exact:
        return identities.project(exact[0])
    workspace_root = (manifest.get("paths") or {}).get("workspace_root")
    workspace = Path(os.path.normpath(os.path.expanduser(workspace_root))) if workspace_root else None
    directories = {} if directories is None else directories
    if folder not in directories:
        directories[folder] = _slug_directory(folder)
    home = directories[folder]
    if home is not None:
        if project := enclosing(home):
            return project
        if workspace is not None and workspace in home.parents:  # named as its file-memory folder names it
            return identities.project(_encode(home.relative_to(workspace).parts[0]))
        return None
    under = max(((len(root.parts), project) for root, project in roots
                 if folder.startswith(_directory_slug(root) + "-")), default=None)
    if under:
        return identities.project(under[1])
    prefix = _directory_slug(workspace) + "-" if workspace is not None else None
    if prefix and folder.startswith(prefix):
        child = folder[len(prefix):]
        # `--` starts a hidden directory, so what precedes it is the child.
        return identities.project(child.split("--")[0] if "--" in child else child)
    return None


def _frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Top-level ``key: value`` pairs of a leading --- block (nested keys flattened), and the body.

    A block holding anything but keys, continuations, list items or comments
    is text between two rules, not frontmatter.
    """
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text
    try:
        end = next(index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---")
    except StopIteration:
        return {}, text
    block = [line for line in lines[1:end] if line.strip()]
    if not block or not all(_FRONTMATTER_LINE.match(line) for line in block):
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


class NotText(ValueError):
    """A file matched as a note holds binary data."""


def _decode(data: bytes) -> str | None:
    """The text of a note file, UTF-8 unless a byte-order mark says otherwise; None if it is not text."""
    encoding = next((name for mark, name in _BOMS if data.startswith(mark)), "utf-8")
    text = data.decode(encoding, errors="replace")
    return None if "\x00" in text else text


def _first_title(body: str) -> str | None:
    """The first level-one heading outside code blocks."""
    fenced = False
    for line in body.splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        elif not fenced and (match := _HEADING.match(line)) and len(match.group(1)) == 1:
            return match.group(2).strip()
    return None


def parse_note(path: Path, data: bytes, project: str | None) -> ParsedNote:
    """A note's fields and chunks, with likely secrets redacted in every stored string (FR-11).

    The whole body is filtered before it is split into sections: filtering each
    section would leave a key that spans two of them, or is longer than one,
    partly in clear text.
    """
    text = _decode(data)
    if text is None:
        raise NotText(str(path))
    fields, body = _frontmatter(text)
    redactions = 0

    def clean(value: str | None) -> str | None:
        nonlocal redactions
        if value is None:
            return None
        result = redact(value)
        redactions += result.count
        return result.text

    body = clean(body) or ""
    title = clean(fields.get("name")) or _first_title(body) or clean(path.stem) or path.stem
    return ParsedNote(
        path=path,
        sha256=hashlib.sha256(data).hexdigest(),
        size=len(data),
        modified=_utc(path.stat().st_mtime),
        project=project,
        title=title,
        description=clean(fields.get("description")),
        kind=clean(fields.get("type")),
        chunks=chunk_note(title, body),
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


def _shown(path: Path | str) -> str:
    """A path as a report may print it."""
    return redact(str(path)).text


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
    directories: dict[str, Path | None] = {}
    paths, linked = discover(settings)
    report["skipped"].extend({"path": _shown(path), "reason": "symlink"} for path in linked)
    for path in paths:
        if denied(path, settings):
            report["denied"] += 1
            continue
        if redact(str(path)).count:  # a path is stored and shown, so it must not hold a key
            report["skipped"].append({"path": _shown(path), "reason": "secret_in_path"})
            continue
        try:
            if path.stat().st_size > MAX_NOTE_BYTES:
                report["skipped"].append({"path": str(path), "reason": "too_large"})
                continue
            data = path.read_bytes()
        except OSError as exc:
            report["skipped"].append({"path": str(path), "reason": type(exc).__name__})
            continue
        project = note_project(path, manifest, identities, directories)
        digest = hashlib.sha256(data).hexdigest()
        if known.get(str(path)) == (digest, project, None):
            live.add(str(path))
            report["unchanged"] += 1
            continue
        try:
            note = parse_note(path, data, project)
        except NotText:
            report["skipped"].append({"path": str(path), "reason": "not_text"})
            continue
        live.add(str(path))
        changed.append(note)
        report["redactions"] += note.redactions
    now = _utc()
    with write_transaction(config) as connection:
        # Text that leaves the index must not stay behind in freed pages.
        connection.execute("PRAGMA secure_delete = ON")
        for note in changed:
            _store(connection, note, now)
            report["indexed"] += 1
        for path, (_, _, deleted) in known.items():
            if path not in live and deleted is None:
                identifier = document_id(Path(path))
                connection.execute("DELETE FROM document_chunks WHERE document_id = ?", (identifier,))
                if redact(path).count:
                    connection.execute("DELETE FROM documents WHERE id = ?", (identifier,))
                else:  # the tombstone keeps the path and nothing the note said
                    connection.execute(
                        "UPDATE documents SET deleted_at_utc = ?, title = ?, description = NULL, kind = NULL, "
                        "content_sha256 = '', size_bytes = 0, redactions = 0 WHERE id = ?",
                        (now, Path(path).stem, identifier))
                report["removed"] += 1
        if changed or report["removed"]:
            # Deleted rows stay in older full-text segments until they merge.
            connection.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES ('optimize')")
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
            # A sync may have re-indexed a chunk while this batch was embedded:
            # store a vector only for the text it was made from.
            embedded += connection.executemany(
                """INSERT INTO chunk_vectors(chunk_id, model_key, dim, vector, created_at_utc)
                   SELECT ?, ?, ?, ?, ? WHERE EXISTS (
                       SELECT 1 FROM document_chunks WHERE id = ? AND heading = ? AND text = ?)
                   ON CONFLICT(chunk_id, model_key) DO NOTHING""",
                [(row["id"], profile.key, len(vector), pack_vector(vector), _utc(), row["id"], row["heading"],
                  row["text"]) for row, vector in zip(batch, vectors)],
            ).rowcount
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
    """A live note's redacted text and fields, as indexed; None for an unknown or deleted note."""
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        row = connection.execute(
            "SELECT * FROM documents WHERE id = ? AND deleted_at_utc IS NULL", (identifier,)).fetchone()
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
    nothing = {"notes": [], "strict": set(), "relaxed": False, "channels": [], "similarities": {},
               "vector_available": False}
    cap = max(limit * 20, 100)
    with open_connection(config) as connection:
        if not _has_documents(connection):
            return nothing
        pool = {row["id"]: dict(row) for row in connection.execute(
            """SELECT c.id, c.document_id, c.heading, c.text, d.path, d.title, d.project, d.kind, d.modified_at_utc,
                      v.dim, v.vector
               FROM document_chunks c JOIN documents d ON d.id = c.document_id AND d.deleted_at_utc IS NULL
               LEFT JOIN chunk_vectors v ON v.chunk_id = c.id AND v.model_key = ?
               WHERE """ + where, (profile.key, *params))}
        if not pool:
            return nothing
        terms = query_terms(query)
        # The scope applies before the cap, or other projects' notes crowd this one's out.
        scoped = ("SELECT document_chunks_fts.chunk_id FROM document_chunks_fts "
                  "JOIN document_chunks c ON c.id = document_chunks_fts.chunk_id "
                  "JOIN documents d ON d.id = c.document_id AND d.deleted_at_utc IS NULL "
                  "WHERE document_chunks_fts MATCH ? AND " + where + " ORDER BY bm25(document_chunks_fts)")
        ranked = [row[0] for row in connection.execute(scoped, (_fts_query(query), *params))] if terms else []
        strict = set(ranked)
        relaxed = bool(terms) and not strict
        fts = ranked[:cap]
        if relaxed:  # a relaxed candidate must still cover the terms; read past those that do not
            rows = connection.execute(scoped, (_fts_query(query, relaxed=True), *params))
            for chunk_id, in itertools.islice(rows, RELAXED_SCAN_PAGES * cap):
                chunk = pool.get(chunk_id)
                if chunk and covers(terms, f"{chunk['heading']} {chunk['text']}"):
                    fts.append(chunk_id)
                    if len(fts) == cap:
                        break
    fts_ranks = {chunk_id: rank for rank, chunk_id in enumerate(fts)}
    similarities: dict[str, float] = {}
    vector_available = False
    if query_vector is not None:
        score = similarity_scorer(query_vector)
        size = len(pack_vector(query_vector))
        for chunk_id, chunk in pool.items():
            # A vector of another size (a malformed row) is skipped, not fatal.
            if chunk["vector"] is not None and chunk["dim"] == len(query_vector) and len(chunk["vector"]) == size:
                vector_available = True
                similarities[chunk_id] = score(unpack_vector(chunk["vector"]))
    admitted = sorted((cid for cid, value in similarities.items() if value >= threshold),
                      key=lambda cid: -similarities[cid])[:cap]
    vector_ranks = {chunk_id: rank for rank, chunk_id in enumerate(admitted)}
    scores = {chunk_id: sum(1 / (60 + rank) for rank in (fts_ranks.get(chunk_id), vector_ranks.get(chunk_id))
                            if rank is not None)
              for chunk_id in set(fts_ranks) | set(vector_ranks)}
    best: dict[str, str] = {}
    for chunk_id in sorted(scores, key=lambda cid: (-scores[cid], cid)):
        best.setdefault(pool[chunk_id]["document_id"], chunk_id)
    # Stable sorts: the newer note breaks a tie (FR-1), and global notes follow project notes.
    ordered = sorted(best.values())
    ordered.sort(key=lambda cid: pool[cid]["modified_at_utc"], reverse=True)
    ordered.sort(key=lambda cid: (scope.project is not None and pool[cid]["project"] is None, -scores[cid]))
    ordered = ordered[:limit]
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
    return {
        "notes": notes,
        "strict": strict,
        "relaxed": relaxed and any(note["channels"]["fts_rank"] is not None for note in notes),
        "channels": (["notes_fts"] if fts else []) + (["notes_vector"] if vector_available else []),
        "similarities": similarities,
        "vector_available": vector_available,
    }


def _has_documents(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'documents'").fetchone() is not None
