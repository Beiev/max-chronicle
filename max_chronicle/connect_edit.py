"""Editing other programs' config files: one contiguous piece of text per change.

`chronicle connect` writes into files that belong to other agents (Claude Code,
Codex, ZCode, opencode, Gemini CLI). Every change here is one contiguous piece
of text inserted, replaced or removed; every byte outside it stays as it was.
Before anything is written, the new text is parsed and compared, in memory,
with the old document plus the intended change: a piece that would alter
anything else is refused.

Formats: JSON, JSONC (comments are masked to spaces of the same length and the
same position-tracking parser reads the rest), TOML (a piece is a whole table
appended at the end, or the block of a table's own header; ``tomllib``
verifies), and plain text (a block between markers).

Refused, with the file untouched and the steps to do it by hand: a symbolic
link, text that is not UTF-8, a byte-order mark, CRLF line ends, duplicate
keys on the path, a TOML target written as a dotted key or an inline table,
JSON5 syntax (trailing commas, single quotes, unquoted keys and the like).

A write is atomic: a temporary file in the same directory, ``fsync``, the old
file's mode, ``os.replace``. It is refused when the file on disk no longer
holds the bytes that were read. A backup ``<file>.bak-YYYYMMDD-HHMMSS`` is
taken before each change. The ledger (``Ledger``) records each change before
it is made: the file, the piece, sha256 before and after.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from json.decoder import scanstring
import os
from pathlib import Path
import re
import tempfile
import time
import tomllib
from typing import Any, Iterator
import uuid

DEFAULT_INDENT = "  "
NEW_CONFIG_MODE = 0o600  # a config file this module creates holds only Chronicle's entry
NEW_TEXT_MODE = 0o644
LEDGER_MODE = 0o600  # a replaced entry's original text may hold a header value
LEDGER_VERSION = 1
LEDGER_LOCK_SUFFIX = ".lock"  # connect-ledger.json -> connect-ledger.lock, beside it
LEDGER_LOCK_WAIT_S = 30.0  # how long a run waits for another run's lock
LEDGER_LOCK_POLL_S = 0.05
BACKUP_TIME_FORMAT = "%Y%m%d-%H%M%S"
_NUMBER = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?")
_LITERALS = {"true": True, "false": False, "null": None}


class EditRefused(Exception):
    """A file this module will not change; nothing was written.

    ``reason`` is the technical cause, ``manual`` what to do by hand.
    """

    def __init__(self, path: Path | str, reason: str, manual: str | None = None) -> None:
        super().__init__(f"{path}: {reason}")
        self.path = str(path)
        self.reason = reason
        self.manual = manual


def sha256_bytes(data: bytes | None) -> str | None:
    return None if data is None else hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str | None:
    try:
        return sha256_bytes(path.read_bytes())
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------------------
# Reading a file
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Loaded:
    """A file as read: its bytes (None when it does not exist) and text."""

    path: Path
    raw: bytes | None
    text: str
    mode: int | None

    @property
    def exists(self) -> bool:
        return self.raw is not None

    @property
    def sha256(self) -> str | None:
        return sha256_bytes(self.raw)


def load(path: Path) -> Loaded:
    """Read *path* for editing, or refuse a file this module does not edit."""
    path = Path(path)
    if path.is_symlink():
        raise EditRefused(path, "is a symbolic link",
                          f"Edit the file the link points to ({os.path.realpath(path)}) by hand.")
    try:
        stat = path.stat()
    except FileNotFoundError:
        return Loaded(path, None, "", None)
    if not path.is_file():
        raise EditRefused(path, "is not a regular file", "Check the path; only regular files are edited.")
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise EditRefused(path, "starts with a byte-order mark",
                          "Remove the byte-order mark, or add the entry by hand.")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EditRefused(path, f"is not UTF-8 ({exc.reason} at byte {exc.start})",
                          "Add the entry by hand.") from exc
    if "\r" in text:
        raise EditRefused(path, "has CRLF line ends", "Add the entry by hand, or convert the file to LF first.")
    return Loaded(path, raw, text, stat.st_mode & 0o7777)


# ---------------------------------------------------------------------------
# An edit and its write
# ---------------------------------------------------------------------------


@dataclass
class Edit:
    """One contiguous change of a file's text: ``old`` at ``start`` becomes ``new``.

    ``op`` is insert, replace, remove, restore, create (a file that did not
    exist) or delete (a file that holds only the piece goes away).
    ``logical`` describes the change for the ledger and for undoing it.
    """

    path: Path
    op: str
    start: int
    old: str
    new: str
    before: Loaded
    after_text: str | None
    logical: dict[str, Any] = field(default_factory=dict)

    @property
    def after_bytes(self) -> bytes | None:
        return None if self.after_text is None else self.after_text.encode("utf-8")

    @property
    def sha256_after(self) -> str | None:
        return sha256_bytes(self.after_bytes)

    def diff(self) -> str:
        """A short unified diff of the change, for the operator to read before it is applied."""
        import difflib

        before = self.before.text.splitlines(keepends=True)
        after = (self.after_text or "").splitlines(keepends=True)
        lines = difflib.unified_diff(before, after, fromfile=str(self.path), tofile=str(self.path), n=2)
        return "".join(lines)


def _splice(text: str, start: int, old: str, new: str) -> str:
    assert text[start:start + len(old)] == old, "the piece to replace is not where it was found"
    return text[:start] + new + text[start + len(old):]


@dataclass(frozen=True)
class WriteResult:
    path: str
    sha256_before: str | None
    sha256_after: str | None
    backup: str | None


def _backup_path(path: Path, now: datetime) -> Path:
    base = f"{path}.bak-{now.strftime(BACKUP_TIME_FORMAT)}"
    candidate, counter = Path(base), 1
    while candidate.exists():
        candidate = Path(f"{base}-{counter}")
        counter += 1
    return candidate


def _fsync_dir(directory: Path) -> None:
    try:
        handle = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(handle)
    except OSError:
        pass
    finally:
        os.close(handle)


def _current_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def write(edit: Edit, *, now: datetime | None = None, backup: bool = True) -> WriteResult:
    """Apply *edit* atomically, after a backup; refuse when the file changed since it was read."""
    path = edit.path
    expected = edit.before.raw
    if path.is_symlink():
        raise EditRefused(path, "became a symbolic link", "Edit it by hand.")
    if _current_bytes(path) != expected:
        raise EditRefused(path, "changed on disk after it was read",
                          "Run the command again; it reads the file anew.")
    now = now or datetime.now()
    backup_path: Path | None = None
    if backup and expected is not None:
        backup_path = _backup_path(path, now)
        with open(backup_path, "xb") as stream:
            stream.write(expected)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(backup_path, edit.before.mode if edit.before.mode is not None else NEW_CONFIG_MODE)
    if edit.after_text is None:  # the file goes away
        if _current_bytes(path) != expected:
            raise EditRefused(path, "changed on disk after it was read",
                              "Run the command again; it reads the file anew.")
        path.unlink()
        _fsync_dir(path.parent)
        return WriteResult(str(path), sha256_bytes(expected), None, str(backup_path) if backup_path else None)
    data = edit.after_bytes or b""
    mode = edit.before.mode
    if mode is None:
        mode = NEW_TEXT_MODE if path.suffix.lower() == ".md" else NEW_CONFIG_MODE
    handle, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".chronicle-tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp, mode)
        if _current_bytes(path) != expected:  # once more, just before the replace
            raise EditRefused(path, "changed on disk after it was read",
                              "Run the command again; it reads the file anew.")
        os.replace(temp, path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)
    return WriteResult(str(path), sha256_bytes(expected), sha256_bytes(data),
                       str(backup_path) if backup_path else None)


# ---------------------------------------------------------------------------
# JSON and JSONC with positions
# ---------------------------------------------------------------------------


class JsonSyntaxError(ValueError):
    pass


@dataclass
class Node:
    kind: str  # object, array or scalar
    start: int
    end: int
    members: list[tuple[str, int, "Node"]] = field(default_factory=list)  # key, key start, value
    items: list["Node"] = field(default_factory=list)

    def member(self, key: str) -> "Node | None":
        found = [value for name, _, value in self.members if name == key]
        return found[-1] if found else None

    def duplicate_keys(self) -> list[str]:
        seen: set[str] = set()
        duplicates: list[str] = []
        for name, _, _ in self.members:
            if name in seen and name not in duplicates:
                duplicates.append(name)
            seen.add(name)
        return duplicates


def mask_comments(text: str) -> tuple[str, bool]:
    """*text* with ``//`` and ``/* */`` comments turned into spaces (newlines kept), and whether any was found."""
    out = list(text)
    index, length, in_string, found = 0, len(text), False, False
    while index < length:
        char = text[index]
        if in_string:
            if char == "\\":
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
        elif char == "/" and text.startswith("//", index):
            found = True
            stop = text.find("\n", index)
            stop = length if stop < 0 else stop
            out[index:stop] = " " * (stop - index)
            index = stop
            continue
        elif char == "/" and text.startswith("/*", index):
            found = True
            stop = text.find("*/", index + 2)
            if stop < 0:
                raise JsonSyntaxError("a /* comment is not closed")
            for position in range(index, stop + 2):
                if text[position] != "\n":
                    out[position] = " "
            index = stop + 2
            continue
        index += 1
    return "".join(out), found


class _Parser:
    def __init__(self, text: str) -> None:
        self.text, self.index = text, 0

    def _where(self) -> str:
        line = self.text.count("\n", 0, self.index) + 1
        column = self.index - (self.text.rfind("\n", 0, self.index) + 1) + 1
        return f"line {line}, column {column}"

    def _fail(self, what: str) -> JsonSyntaxError:
        return JsonSyntaxError(f"{what} at {self._where()}")

    def _skip(self) -> None:
        text, index = self.text, self.index
        while index < len(text) and text[index] in " \t\n\r":
            index += 1
        self.index = index

    def _peek(self) -> str:
        self._skip()
        if self.index >= len(self.text):
            raise self._fail("unexpected end")
        return self.text[self.index]

    def _string(self) -> str:
        try:
            value, end = scanstring(self.text, self.index + 1, True)
        except ValueError as exc:
            raise self._fail(f"bad string ({exc})") from exc
        self.index = end
        return value

    def document(self) -> Node:
        node = self.value()
        self._skip()
        if self.index != len(self.text):
            raise self._fail("text after the end of the document")
        return node

    def value(self) -> Node:
        char = self._peek()
        start = self.index
        if char == "{":
            return self._object()
        if char == "[":
            return self._array()
        if char == '"':
            self._string()
            return Node("scalar", start, self.index)
        match = _NUMBER.match(self.text, self.index)
        if match and match.end() > start:
            self.index = match.end()
            return Node("scalar", start, self.index)
        for literal in _LITERALS:
            if self.text.startswith(literal, self.index):
                self.index += len(literal)
                return Node("scalar", start, self.index)
        raise self._fail(f"unexpected {char!r} (JSON5 or not JSON)")

    def _object(self) -> Node:
        node = Node("object", self.index, -1)
        self.index += 1
        if self._peek() == "}":
            self.index += 1
            node.end = self.index
            return node
        while True:
            if self._peek() != '"':
                raise self._fail("a key that is not a double-quoted string (JSON5 or not JSON)")
            key_start = self.index
            key = self._string()
            if self._peek() != ":":
                raise self._fail("expected ':'")
            self.index += 1
            node.members.append((key, key_start, self.value()))
            char = self._peek()
            if char == ",":
                self.index += 1
                if self._peek() == "}":
                    raise self._fail("a trailing comma (JSON5)")
                continue
            if char == "}":
                self.index += 1
                node.end = self.index
                return node
            raise self._fail("expected ',' or '}'")

    def _array(self) -> Node:
        node = Node("array", self.index, -1)
        self.index += 1
        if self._peek() == "]":
            self.index += 1
            node.end = self.index
            return node
        while True:
            node.items.append(self.value())
            char = self._peek()
            if char == ",":
                self.index += 1
                if self._peek() == "]":
                    raise self._fail("a trailing comma (JSON5)")
                continue
            if char == "]":
                self.index += 1
                node.end = self.index
                return node
            raise self._fail("expected ',' or ']'")


def _line_start(text: str, position: int) -> int:
    return text.rfind("\n", 0, position) + 1


def _indent_at(text: str, position: int) -> str:
    start = _line_start(text, position)
    end = start
    while end < len(text) and text[end] in " \t":
        end += 1
    return text[start:end]


def _document_indent(text: str) -> str:
    """The indent unit of a document: its smallest leading whitespace on a line with JSON on it."""
    best: str | None = None
    for line in text.splitlines():
        stripped = line.lstrip(" \t")
        if not stripped or stripped[0] not in '"{}[]' or len(stripped) == len(line):
            continue
        lead = line[: len(line) - len(stripped)]
        if lead.startswith("\t"):
            return "\t"
        if best is None or len(lead) < len(best):
            best = lead
    return best or DEFAULT_INDENT


def _render(value: Any, base_indent: str, unit: str) -> str:
    return json.dumps(value, indent=unit, ensure_ascii=False).replace("\n", "\n" + base_indent)


def _render_member(key: str, value: Any, base_indent: str, unit: str) -> str:
    return f"{json.dumps(key, ensure_ascii=False)}: {_render(value, base_indent, unit)}"


def _nest(path: list[str], value: Any) -> Any:
    for key in reversed(path):
        value = {key: value}
    return value


def _get(document: Any, path: list[str | int]) -> Any:
    """The value at *path* (keys of objects, indices of arrays), or None."""
    for key in path:
        if isinstance(key, int):
            if not isinstance(document, list) or not 0 <= key < len(document):
                return None
        elif not isinstance(document, dict) or key not in document:
            return None
        document = document[key]
    return document


def _dotted(path: list[str | int]) -> str:
    return ".".join(str(key) for key in path) or "(root)"


class JsonDoc:
    """A JSON or JSONC document read with the position of every value."""

    def __init__(self, loaded: Loaded, *, comments: bool) -> None:
        self.loaded = loaded
        self.path = loaded.path
        self.comments = comments
        text = loaded.text
        self.text = text
        if not loaded.exists or not text.strip():
            self.root: Node | None = None
            self.masked = text
            self.value: Any = None
            return
        try:
            masked, found = mask_comments(text)
            if found and not comments:
                raise JsonSyntaxError("comments in a file that must be strict JSON")
            self.root = _Parser(masked).document()
            self.value = json.loads(masked)
        except (JsonSyntaxError, ValueError) as exc:
            raise EditRefused(self.path, f"cannot be read as {'JSONC' if comments else 'JSON'}: {exc}",
                              "Fix the file or add the entry by hand.") from exc
        self.masked = masked
        if self.root.kind != "object":
            raise EditRefused(self.path, "does not hold a JSON object", "Add the entry by hand.")

    # -- reading ----------------------------------------------------------

    def node(self, path: list[str | int]) -> Node | None:
        node = self.root
        for key in path:
            if node is None:
                return None
            if isinstance(key, int):
                if node.kind != "array" or not 0 <= key < len(node.items):
                    return None
                node = node.items[key]
            elif node.kind != "object":
                return None
            else:
                node = node.member(key)
        return node

    def get(self, path: list[str]) -> Any:
        return _get(self.value, path)

    def check_path(self, path: list[str | int]) -> None:
        """Refuse when an object on *path* (the root included) repeats a key, or a step is not an object."""
        node = self.root
        walked: list[str | int] = []
        for key in [*path, None]:
            if node is None:
                return
            if isinstance(key, int):
                if node.kind != "array":
                    raise EditRefused(self.path, f"'{_dotted(walked)}' is not an array", "Edit it by hand.")
                walked.append(key)
                node = node.items[key] if 0 <= key < len(node.items) else None
                continue
            if node.kind != "object":
                if key is None:
                    return
                raise EditRefused(self.path, f"'{'.'.join(walked) or '(root)'}' is not an object",
                                  "Add the entry by hand.")
            duplicates = node.duplicate_keys()
            if duplicates:
                raise EditRefused(self.path, f"duplicate key {duplicates[0]!r} in "
                                  f"'{'.'.join(walked) or '(root)'}'",
                                  "Remove the duplicate key, or add the entry by hand.")
            if key is None:
                return
            walked.append(key)
            node = node.member(key)

    def text_of(self, node: Node) -> str:
        return self.text[node.start:node.end]

    # -- planning ---------------------------------------------------------

    def _new_file(self, value: dict[str, Any], logical: dict[str, Any]) -> Edit:
        """The whole text of a file that is missing or holds no JSON value: Chronicle's entry after what it held.

        The ledger keeps that text (``before_text``): `disconnect` puts the file
        back to it, or removes a file this created.
        """
        content = json.dumps(value, indent=DEFAULT_INDENT, ensure_ascii=False) + "\n"
        op = "create" if not self.loaded.exists else "insert"
        logical["new_file"] = True
        logical["before_text"] = self.text
        edit = Edit(self.path, op, len(self.text), "", content, self.loaded, self.text + content, logical)
        self._verify(edit, value)
        return edit

    def _unit(self, container: Node) -> str:
        if container.members or container.items:
            first = container.members[0][1] if container.members else container.items[0].start
            if "\n" in self.masked[container.start:first]:
                inner = _indent_at(self.text, first)
                outer = _indent_at(self.text, container.start)
                if inner.startswith(outer) and len(inner) > len(outer):
                    return inner[len(outer):]
        return _document_indent(self.text)

    def _insertion(self, container: Node, rendered: list[str], inline: list[str], *,
                   first: bool = False) -> tuple[int, str]:
        """Where and what to insert so that *rendered* entries end the container (or start it)."""
        entries_end = [value.end for _, _, value in container.members] or [item.end for item in container.items]
        outer = _indent_at(self.text, container.start)
        unit = self._unit(container)
        if not entries_end:  # an empty container
            inner = outer + unit
            between = self.masked[container.start + 1:container.end - 1]
            body = ("," + "\n" + inner).join(rendered)
            piece = "\n" + inner + body + ("" if "\n" in between else "\n" + outer)
            return container.start + 1, piece
        place_first = first
        first = container.members[0][1] if container.members else container.items[0].start
        if "\n" not in self.masked[container.start:first]:  # entries written on the bracket's line
            return entries_end[-1], "".join(", " + item for item in inline)
        inner = _indent_at(self.text, first)
        if place_first:
            return container.start + 1, "".join("\n" + inner + item + "," for item in rendered)
        last = entries_end[-1]
        line_end = self.text.find("\n", last)
        line_end = len(self.text) if line_end < 0 else line_end
        tail_masked = self.masked[last:line_end]
        tail_text = self.text[last:line_end]
        if tail_masked.strip(" \t,") == "" and tail_text.strip(" \t,") != "":
            # A comment follows the last entry on its line: start the container instead.
            return container.start + 1, "".join("\n" + inner + item + "," for item in rendered)
        return last, "".join(",\n" + inner + item for item in rendered)

    def _verify(self, edit: Edit, expected: Any) -> None:
        after = edit.after_text or ""
        try:
            masked, found = mask_comments(after)
            if found and not self.comments:
                raise JsonSyntaxError("comments in a file that must be strict JSON")
            _Parser(masked).document()
            value = json.loads(masked)
        except (JsonSyntaxError, ValueError) as exc:
            raise EditRefused(self.path, f"the changed text would not parse: {exc}",
                              "Add the entry by hand.") from exc
        if value != expected:
            raise EditRefused(self.path, "the change would alter more than the entry",
                              "Add the entry by hand.")

    def insert_member(self, path: list[str], key: str, value: Any, *, logical: dict[str, Any] | None = None,
                      first: bool = False) -> Edit:
        """Add *key* = *value* to the object at *path*, creating the missing objects of the path.

        *first* puts it before the object's other members, where its owner writes it.
        """
        logical = {"format": "json", "action": "member", "path": list(path), "key": key, "value": value,
                   **(logical or {})}
        if self.root is None:
            return self._new_file(_nest(path, {key: value}), logical)
        self.check_path(path)
        depth = 0
        node = self.root
        while depth < len(path) and node.member(path[depth]) is not None:
            node = node.member(path[depth])
            if node.kind != "object":
                raise EditRefused(self.path, f"'{'.'.join(path[:depth + 1])}' is not an object",
                                  "Add the entry by hand.")
            depth += 1
        if depth == len(path) and node.member(key) is not None:
            raise EditRefused(self.path, f"'{'.'.join([*path, key])}' exists already", "Edit it by hand.")
        new_key = path[depth] if depth < len(path) else key
        new_value = _nest(path[depth + 1:], {key: value}) if depth < len(path) else value
        outer = _indent_at(self.text, node.start)
        unit = self._unit(node)
        inner = outer + unit
        if node.members and "\n" in self.masked[node.start:node.members[0][1]]:
            inner = _indent_at(self.text, node.members[0][1])
        rendered = _render_member(new_key, new_value, inner, unit)
        inline = json.dumps({new_key: new_value}, ensure_ascii=False, separators=(", ", ": "))[1:-1]
        start, piece = self._insertion(node, [rendered], [inline], first=first)
        logical["created"] = list(path[:depth + 1]) if depth < len(path) else None
        edit = Edit(self.path, "insert", start, "", piece, self.loaded,
                    self.text[:start] + piece + self.text[start:], logical)
        expected = copy.deepcopy(self.value)
        target = _get(expected, path[:depth]) if depth else expected
        target[new_key] = new_value
        self._verify(edit, expected)
        return edit

    def append_items(self, path: list[str], items: list[Any], *, logical: dict[str, Any] | None = None,
                     first: bool = False) -> Edit:
        """Append *items* to the array at *path*; a missing array is created with them (*first*: as the first member)."""
        if not items:
            raise ValueError("nothing to append")
        node = self.node(path) if self.root is not None else None
        if node is None:  # the array is new: the change is its member, undone by removing it
            return self.insert_member(path[:-1], path[-1], list(items), logical={"array": list(path), **(logical or {})},
                                      first=first)
        logical = {"format": "json", "action": "items", "path": list(path), "items": list(items), **(logical or {})}
        self.check_path(path)
        if node.kind != "array":
            raise EditRefused(self.path, f"'{'.'.join(path)}' is not an array", "Add the entries by hand.")
        unit = self._unit(node)
        outer = _indent_at(self.text, node.start)
        inner = outer + unit
        if node.items and "\n" in self.masked[node.start:node.items[0].start]:
            inner = _indent_at(self.text, node.items[0].start)
        rendered = [_render(item, inner, unit) for item in items]
        inline = [json.dumps(item, ensure_ascii=False, separators=(", ", ": ")) for item in items]
        start, piece = self._insertion(node, rendered, inline)
        edit = Edit(self.path, "insert", start, "", piece, self.loaded,
                    self.text[:start] + piece + self.text[start:], logical)
        expected = copy.deepcopy(self.value)
        array = _get(expected, path)
        if start == node.start + 1 and node.items:  # placed first, see _insertion
            array[:0] = copy.deepcopy(items)
        else:
            array.extend(copy.deepcopy(items))
        self._verify(edit, expected)
        return edit

    def replace_value(self, path: list[str | int], value: Any, *, logical: dict[str, Any] | None = None) -> Edit:
        """Replace the value at *path* with *value* (the key and everything around it stay)."""
        self.check_path(path[:-1])
        node = self.node(path)
        if node is None:
            raise EditRefused(self.path, f"'{'.'.join(path)}' does not exist", "Add the entry by hand.")
        old_text = self.text_of(node)
        if self.masked[node.start:node.end] != old_text:
            raise EditRefused(self.path, f"'{'.'.join(path)}' holds comments", "Edit the entry by hand.")
        unit = self._unit(node) if node.kind != "scalar" else _document_indent(self.text)
        base = _indent_at(self.text, node.start)
        if "\n" in old_text or node.kind == "scalar":
            new_text = _render(value, base, unit)
        else:
            new_text = json.dumps(value, ensure_ascii=False, separators=(", ", ": "))
        logical = {"format": "json", "action": "replace", "path": list(path), "value": value,
                   "original_value": _get(self.value, path), **(logical or {})}
        edit = Edit(self.path, "replace", node.start, old_text, new_text, self.loaded,
                    _splice(self.text, node.start, old_text, new_text), logical)
        expected = copy.deepcopy(self.value)
        _get(expected, path[:-1])[path[-1]] = value
        self._verify(edit, expected)
        return edit

    def rename_key(self, path: list[str], old: str, new: str, *, logical: dict[str, Any] | None = None) -> Edit:
        """Rename the member *old* of the object at *path* to *new*; its value stays as written."""
        self.check_path(path)
        node = self.node(path)
        if node is None or node.kind != "object":
            raise EditRefused(self.path, f"'{_dotted(path)}' is not an object", "Rename it by hand.")
        if node.member(new) is not None:
            raise EditRefused(self.path, f"'{_dotted([*path, new])}' exists already", "Rename it by hand.")
        starts = [key_start for key, key_start, _ in node.members if key == old]
        if len(starts) != 1:
            raise EditRefused(self.path, f"'{_dotted([*path, old])}' does not exist once", "Rename it by hand.")
        start = starts[0]
        _, end = scanstring(self.masked, start + 1, True)
        old_token = self.text[start:end]
        new_token = json.dumps(new, ensure_ascii=False)
        logical = {"format": "json", "action": "rename", "path": list(path), "old": old, "new": new,
                   **(logical or {})}
        edit = Edit(self.path, "replace", start, old_token, new_token, self.loaded,
                    _splice(self.text, start, old_token, new_token), logical)
        expected = copy.deepcopy(self.value)
        parent = _get(expected, path) if path else expected
        parent[new] = parent.pop(old)
        self._verify(edit, expected)
        return edit

    def undo(self, change: dict[str, Any]) -> Edit:
        """The edit that takes a recorded change back out of the current text.

        A file the change created, or one that held no JSON value before it, goes
        back to its exact first bytes (a created file is removed) when nothing
        else was added to it.
        """
        logical = change["logical"]
        if logical.get("new_file"):
            before = logical.get("before_text", "")
            if self.text == before + change["piece"]:  # the file holds only Chronicle's entry
                return self._first_state(change, before)
            edit = self._take_out_new(change)
            if self._holds_nothing_else(edit.after_text, logical):
                return self._first_state(change, before)
            return edit
        return self._take_out(change)

    def _take_out_new(self, change: dict[str, Any]) -> Edit:
        """A new file's entry taken out with the outermost of its objects that holds nothing else."""
        logical = change["logical"]
        path = list(logical.get("path") or [])
        for depth in range(1, len(path) + 1):
            try:
                return self._take_out(change, {**logical, "created": path[:depth]})
            except EditRefused:
                continue
        return self._take_out(change)

    def _first_state(self, change: dict[str, Any], before: str) -> Edit:
        if change["op"] == "create":
            return Edit(self.path, "delete", 0, self.text, "", self.loaded, None, change["logical"])
        return Edit(self.path, "restore", 0, self.text, before, self.loaded, before, change["logical"])

    def _holds_nothing_else(self, text: str | None, logical: dict[str, Any]) -> bool:
        """Whether *text* (a new file with the change taken out) holds only the empty objects of its path."""
        if text is None:
            return True
        masked, comments = mask_comments(text)
        if comments:
            return False
        try:
            value = json.loads(masked) if masked.strip() else {}
        except ValueError:
            return False
        path = list(logical.get("path") or [])
        for depth in range(len(path), 0, -1):
            parent = _get(value, path[:depth - 1]) if depth > 1 else value
            if isinstance(parent, dict) and parent.get(path[depth - 1]) == {}:
                del parent[path[depth - 1]]
        return value == {}

    def _take_out(self, change: dict[str, Any], logical: dict[str, Any] | None = None) -> Edit:
        """The piece taken out where it was written, else where the owner moved it."""
        logical = logical or change["logical"]
        piece, original = change["piece"], change.get("original", "")
        candidates = _occurrences(self.text, piece, change.get("offset"))
        for start in candidates:
            after = _splice(self.text, start, piece, original)
            try:
                masked, _ = mask_comments(after)
                value = json.loads(masked) if masked.strip() else None
            except (JsonSyntaxError, ValueError):
                continue
            if value == self._undone(logical):
                op = "restore" if original else "remove"
                return Edit(self.path, op, start, piece, original, self.loaded, after, logical)
        # The owner rewrote the file (another key order, other members) and kept the
        # entry: remove it where it is now, as one contiguous piece, verified alike.
        found = self._structural(logical, original)
        if found is not None:
            start, old, new = found
            after = _splice(self.text, start, old, new)
            try:
                masked, _ = mask_comments(after)
                value = json.loads(masked) if masked.strip() else None
            except (JsonSyntaxError, ValueError):
                value = object()
            if value == self._undone(logical):
                return Edit(self.path, "restore" if new else "remove", start, old, new, self.loaded, after,
                            {**logical, "structural": True})
        raise EditRefused(self.path, "the entry Chronicle wrote is no longer there as written",
                          _manual_undo(logical))

    def has_trace(self, logical: dict[str, Any]) -> bool:
        """Whether anything of a recorded change is still in the file (when it cannot be taken back as written)."""
        if self.root is None:
            return False
        action = logical.get("action")
        if action == "member":
            created = logical.get("created")
            return self.node(created or [*logical["path"], logical["key"]]) is not None
        if action == "items":
            node = self.node(logical["path"])
            if node is None or node.kind != "array":
                return False
            values = [self._value_of(item) for item in node.items]
            return any(item in values for item in logical["items"])
        if action == "replace":
            node = self.node(logical["path"])
            return node is not None and self._value_of(node) != logical.get("original_value")
        if action == "rename":
            return self.node([*logical["path"], logical["new"]]) is not None
        return True

    def _value_of(self, node: Node) -> Any:
        return json.loads(self.masked[node.start:node.end])

    def _removal(self, container: Node, first: int, last: int) -> tuple[int, int] | None:
        """The span that removes entries *first*..*last* of *container* with one comma; None across a comment."""
        starts = [key_start for _, key_start, _ in container.members] or [item.start for item in container.items]
        ends = [value.end for _, _, value in container.members] or [item.end for item in container.items]
        count = len(starts)
        if first == 0 and last == count - 1:
            span = (container.start + 1, container.end - 1)
        elif last < count - 1:
            span = (starts[first], starts[last + 1])
        else:
            span = (ends[first - 1], ends[last])
        if self.masked[span[0]:span[1]] != self.text[span[0]:span[1]]:
            return None
        return span

    def _structural(self, logical: dict[str, Any], original: str) -> tuple[int, str, str] | None:
        """Where a recorded entry is in the current text, found by its path and value: (start, old, new)."""
        action = logical.get("action")
        if self.root is None:
            return None
        if action == "member":
            created = logical.get("created")
            if created:
                path, key = created[:-1], created[-1]
                value = _nest(logical["path"][len(created):], {logical["key"]: logical["value"]})
            else:
                path, key, value = logical["path"], logical["key"], logical["value"]
            container = self.node(path) if path else self.root
            if container is None or container.kind != "object":
                return None
            indices = [index for index, (name, _, node) in enumerate(container.members)
                       if name == key and self._value_of(node) == value]
            if len(indices) != 1:
                return None
            span = self._removal(container, indices[0], indices[0])
        elif action == "items":
            container = self.node(logical["path"])
            items = logical["items"]
            if container is None or container.kind != "array":
                return None
            values = [self._value_of(node) for node in container.items]
            matches = [start for start in range(len(values) - len(items), -1, -1)
                       if values[start:start + len(items)] == items]
            if not matches:
                return None
            span = self._removal(container, matches[0], matches[0] + len(items) - 1)
        elif action == "replace":
            node = self.node(logical["path"])
            if node is None or self._value_of(node) != logical["value"]:
                return None
            return node.start, self.text[node.start:node.end], original
        elif action == "rename":
            container = self.node(logical["path"]) if logical["path"] else self.root
            if container is None or container.kind != "object":
                return None
            starts = [key_start for name, key_start, _ in container.members if name == logical["new"]]
            if len(starts) != 1:
                return None
            _, end = scanstring(self.masked, starts[0] + 1, True)
            return starts[0], self.text[starts[0]:end], original
        else:
            return None
        if span is None:
            return None
        return span[0], self.text[span[0]:span[1]], ""

    def _undone(self, logical: dict[str, Any]) -> Any:
        expected = copy.deepcopy(self.value)
        action = logical["action"]
        if action == "rename":
            parent = _get(expected, logical["path"]) if logical["path"] else expected
            if not isinstance(parent, dict) or logical["new"] not in parent:
                return object()
            parent[logical["old"]] = parent.pop(logical["new"])
            return expected
        if action == "replace":
            parent = _get(expected, logical["path"][:-1])
            if not isinstance(parent, dict):
                return object()
            parent[logical["path"][-1]] = logical["original_value"]
            return expected
        created = logical.get("created")
        if created:  # the change made the objects of this path: they go together
            parent = _get(expected, created[:-1]) if len(created) > 1 else expected
            if not isinstance(parent, dict) or created[-1] not in parent:
                return object()
            del parent[created[-1]]
            return expected
        if action == "member":
            parent = _get(expected, logical["path"])
            if not isinstance(parent, dict) or logical["key"] not in parent:
                return object()
            del parent[logical["key"]]
            return expected
        if action == "items":
            array = _get(expected, logical["path"])
            items = logical["items"]
            if not isinstance(array, list):
                return object()
            for start in range(len(array) - len(items), -1, -1):
                if array[start:start + len(items)] == items:
                    del array[start:start + len(items)]
                    return expected
            return object()
        return object()


def _occurrences(text: str, piece: str, offset: int | None) -> list[int]:
    found: list[int] = []
    if offset is not None and 0 <= offset and text[offset:offset + len(piece)] == piece:
        found.append(offset)
    start = text.find(piece)
    while start >= 0 and piece:
        if start not in found:
            found.append(start)
        start = text.find(piece, start + 1)
    return found


def _manual_undo(logical: dict[str, Any]) -> str:
    fmt, action = logical.get("format"), logical.get("action")
    if fmt == "json":
        where = ".".join(logical.get("path") or []) or "(root)"
        if action == "member":
            return f"Remove the key {logical['key']!r} under '{where}' by hand."
        if action == "items":
            return f"Remove these entries from '{where}' by hand: {json.dumps(logical['items'])}"
        if action == "replace":
            return f"Set '{where}' back by hand to: {json.dumps(logical.get('original_value'))}"
    if fmt == "toml":
        if action == "value":
            return (f"Set {logical['key']} in [{toml_key(logical['name'])}] back by hand to "
                    f"{toml_string(logical['original_value'])}.")
        if action == "rename":
            return f"Rename [{toml_key(logical['new'])}] back to [{toml_key(logical['name'])}] by hand."
        return "Remove these tables by hand:\n" + str(logical.get("core", ""))
    if fmt == "text":
        return (f"Remove the lines from {logical.get('begin')!r} to {logical.get('end')!r} by hand.")
    return "Undo the change by hand."


# ---------------------------------------------------------------------------
# TOML
# ---------------------------------------------------------------------------

_HEADER = re.compile(r"^[ \t]*\[(?!\[)(?P<name>[^\]\n]*)\][ \t]*(?:#[^\n]*)?$", re.MULTILINE)
# A key line whose value is one single-line string: `key = "..."` or `key = '...'`.
_STRING_LINE = re.compile(r"""^[ \t]*(?P<key>[A-Za-z0-9_-]+|"[^"\\\n]*")[ \t]*=[ \t]*"""
                          r"""(?P<value>"(?:[^"\\\n]|\\.)*"|'[^'\n]*')(?P<rest>[^\n]*)$""", re.MULTILINE)
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _split_key(name: str) -> tuple[str, ...] | None:
    """The parts of a dotted TOML key (``a."b c".d``), or None when it cannot be read."""
    parts: list[str] = []
    index, text = 0, name.strip()
    while True:
        while index < len(text) and text[index] in " \t":
            index += 1
        if index >= len(text):
            return None
        if text[index] in "\"'":
            quote = text[index]
            stop = text.find(quote, index + 1)
            if stop < 0:
                return None
            raw = text[index + 1:stop]
            if quote == '"':
                try:
                    raw = json.loads(f'"{raw}"')
                except ValueError:
                    return None
            parts.append(raw)
            index = stop + 1
        else:
            match = _BARE_KEY.match(text, index)
            if not match:
                return None
            parts.append(match.group(0))
            index = match.end()
        while index < len(text) and text[index] in " \t":
            index += 1
        if index >= len(text):
            return tuple(parts)
        if text[index] != ".":
            return None
        index += 1


def toml_key(parts: tuple[str, ...] | list[str]) -> str:
    return ".".join(part if _BARE_KEY.fullmatch(part) else json.dumps(part) for part in parts)


def toml_string(value: str) -> str:
    return json.dumps(value)  # a TOML basic string: JSON's escapes are valid there


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any] | None:
    merged = copy.deepcopy(base)
    for key, value in extra.items():
        if key in merged:
            if isinstance(merged[key], dict) and isinstance(value, dict):
                inner = _deep_merge(merged[key], value)
                if inner is None:
                    return None
                merged[key] = inner
            else:
                return None
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _subtract(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any] | None:
    """*base* without the keys of *extra*; tables left empty by that go too."""
    result = copy.deepcopy(base)
    for key, value in extra.items():
        if key not in result:
            return None
        if isinstance(value, dict) and value and isinstance(result[key], dict):
            inner = _subtract(result[key], value)
            if inner is None:
                return None
            if inner:
                result[key] = inner
            else:
                del result[key]
        else:
            del result[key]
    return result


def _prune(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _prune(item) for key, item in value.items() if not (isinstance(item, dict) and not _prune(item))}
    return value


class TomlDoc:
    """A TOML document; changes are whole tables appended at the end or a table's own block."""

    def __init__(self, loaded: Loaded) -> None:
        self.loaded = loaded
        self.path = loaded.path
        self.text = loaded.text
        try:
            self.value = tomllib.loads(self.text)
        except tomllib.TOMLDecodeError as exc:
            raise EditRefused(self.path, f"cannot be read as TOML: {exc}", "Fix the file or add the entry by hand.") from exc

    def get(self, path: list[str]) -> Any:
        return _get(self.value, path)

    def headers(self) -> list[tuple[tuple[str, ...] | None, int, int]]:
        """Every ``[table]`` header: its key, the start of its line and of the next line."""
        found = []
        for match in _HEADER.finditer(self.text):
            end = match.end() + 1 if match.end() < len(self.text) else match.end()
            found.append((_split_key(match.group("name")), match.start(), end))
        return found

    def check_table(self, name: tuple[str, ...], *, own_header: bool = False) -> None:
        """Refuse when *name* or a table above it is written as a dotted key or an inline table.

        A table counts as written with headers when it has its own header or a
        sub-table header; *own_header* asks for *name*'s own ``[name]`` line.
        """
        for depth in range(1, len(name) + 1):
            prefix = name[:depth]
            value = _get(self.value, list(prefix))
            if value is None:
                return
            if not isinstance(value, dict):
                raise EditRefused(self.path, f"'{toml_key(prefix)}' is not a table", "Add the entry by hand.")
            has_header = any(key == prefix for key, _, _ in self.headers())
            has_children = any(key is not None and key[:depth] == prefix and len(key) > depth
                               for key, _, _ in self.headers())
            if not has_header and not has_children:
                raise EditRefused(
                    self.path, f"'{toml_key(prefix)}' is written as a dotted key or an inline table",
                    f"Add the [{toml_key(name)}] table by hand.")
            if own_header and not has_header and depth == len(name):
                raise EditRefused(
                    self.path, f"'{toml_key(prefix)}' is written as a dotted key or an inline table",
                    f"Add the [{toml_key(name)}] table by hand.")

    def block(self, name: tuple[str, ...]) -> tuple[int, int] | None:
        """The text of table *name*'s own block: its header line up to its last content line."""
        headers = self.headers()
        for index, (key, start, body) in enumerate(headers):
            if key != name:
                continue
            stop = headers[index + 1][1] if index + 1 < len(headers) else len(self.text)
            lines = self.text[body:stop].split("\n")
            keep = body
            position = body
            for line in lines:
                stripped = line.strip()
                line_end = position + len(line)
                if stripped and not stripped.startswith("#"):
                    keep = min(line_end + 1, len(self.text))
                position = line_end + 1
            return start, keep
        return None

    def _verify(self, after: str, expected: Any) -> None:
        try:
            value = tomllib.loads(after)
        except tomllib.TOMLDecodeError as exc:
            raise EditRefused(self.path, f"the changed text would not parse: {exc}", "Add the entry by hand.") from exc
        if _prune(value) != _prune(expected):
            raise EditRefused(self.path, "the change would alter more than the entry", "Add the entry by hand.")

    def append(self, core: str, *, logical: dict[str, Any] | None = None) -> Edit:
        """Append the tables of *core* at the end of the file."""
        try:
            addition = tomllib.loads(core)
        except tomllib.TOMLDecodeError as exc:  # pragma: no cover - the caller's own text
            raise ValueError(f"not TOML: {exc}") from exc
        for name in _table_names(addition):
            if len(name) > 1:
                self.check_table(name[:-1])
        text = self.text
        if not text:
            separator = ""
        elif text.endswith("\n\n"):
            separator = ""
        elif text.endswith("\n"):
            separator = "\n"
        else:
            separator = "\n\n"
        piece = separator + core
        after = text + piece
        expected = _deep_merge(self.value, addition)
        if expected is None:
            raise EditRefused(self.path, "the tables exist already", "Edit them by hand.")
        logical = {"format": "toml", "action": "tables", "core": core, **(logical or {})}
        edit = Edit(self.path, "create" if not self.loaded.exists else "insert", len(text), "", piece,
                    self.loaded, after, logical)
        self._verify(after, expected)
        return edit

    def replace_block(self, name: tuple[str, ...], core: str, *, logical: dict[str, Any] | None = None) -> Edit:
        """Replace the own block of table *name* (header and keys, not its sub-tables) with *core*."""
        self.check_table(name, own_header=True)
        span = self.block(name)
        if span is None:
            raise EditRefused(self.path, f"[{toml_key(name)}] has no header", "Edit it by hand.")
        start, stop = span
        old = self.text[start:stop]
        new = core if old.endswith("\n") or stop < len(self.text) else core.rstrip("\n")
        after = _splice(self.text, start, old, new)
        replacement = _get(tomllib.loads(core), list(name)) or {}
        expected = copy.deepcopy(self.value)
        table = _get(expected, list(name))
        for key in [key for key, value in table.items() if not isinstance(value, dict)]:
            del table[key]
        table.update(copy.deepcopy(replacement))
        logical = {"format": "toml", "action": "block", "name": list(name), "core": core,
                   "original_table": {k: v for k, v in _get(self.value, list(name)).items() if not isinstance(v, dict)},
                   **(logical or {})}
        edit = Edit(self.path, "replace", start, old, new, self.loaded, after, logical)
        self._verify(after, expected)
        return edit

    def replace_string(self, name: tuple[str, ...], key: str, value: str, *,
                       logical: dict[str, Any] | None = None) -> Edit:
        """Replace the string value of *key* in table *name*'s own block; every other byte stays."""
        self.check_table(name, own_header=True)
        span = self.block(name)
        if span is None:
            raise EditRefused(self.path, f"[{toml_key(name)}] has no header", "Edit it by hand.")
        start, stop = span
        found = [match for match in _STRING_LINE.finditer(self.text, start, stop)
                 if _split_key(match.group("key")) == (key,)
                 and (not match.group("rest").strip() or match.group("rest").strip().startswith("#"))]
        where = f"{key} in [{toml_key(name)}]"
        original = (_get(self.value, list(name)) or {}).get(key)
        if len(found) != 1 or not isinstance(original, str):
            raise EditRefused(self.path, f"{where} is not one single-line string", f"Set {where} by hand.")
        match = found[0]
        old, new = match.group("value"), toml_string(value)
        after = _splice(self.text, match.start("value"), old, new)
        expected = copy.deepcopy(self.value)
        _get(expected, list(name))[key] = value
        logical = {"format": "toml", "action": "value", "name": list(name), "key": key, "value": value,
                   "original_value": original, **(logical or {})}
        edit = Edit(self.path, "replace", match.start("value"), old, new, self.loaded, after, logical)
        self._verify(after, expected)
        return edit

    def rename_table(self, name: tuple[str, ...], new: tuple[str, ...], *,
                     logical: dict[str, Any] | None = None) -> Edit:
        """Rename table *name* to *new* by its header line; a table with sub-tables is renamed by hand."""
        self.check_table(name, own_header=True)
        if _get(self.value, list(new)) is not None:
            raise EditRefused(self.path, f"[{toml_key(new)}] exists already", "Rename it by hand.")
        headers = self.headers()
        if any(key is not None and key[:len(name)] == name and len(key) > len(name) for key, _, _ in headers):
            raise EditRefused(self.path, f"[{toml_key(name)}] has sub-tables", "Rename it and its sub-tables by hand.")
        (start, body), = [(start, body) for key, start, body in headers if key == name]
        line = self.text[start:body]
        new_line = f"[{toml_key(new)}]" + ("\n" if line.endswith("\n") else "")
        after = _splice(self.text, start, line, new_line)
        expected = copy.deepcopy(self.value)
        parent = _get(expected, list(name[:-1])) if len(name) > 1 else expected
        table = parent.pop(name[-1])
        target = _get(expected, list(new[:-1])) if len(new) > 1 else expected
        target[new[-1]] = table
        logical = {"format": "toml", "action": "rename", "name": list(name), "new": list(new), **(logical or {})}
        edit = Edit(self.path, "replace", start, line, new_line, self.loaded, after, logical)
        self._verify(after, expected)
        return edit

    def has_trace(self, logical: dict[str, Any]) -> bool:
        """Whether anything of a recorded change is still in the file (when it cannot be taken back as written)."""
        action = logical.get("action")
        if action == "tables":
            names = _table_names(tomllib.loads(logical["core"]))
            leaves = [name for name in names if not any(other[:len(name)] == name and other != name for other in names)]
            return any(_get(self.value, list(name)) is not None for name in leaves)
        if action == "block":
            return _get(self.value, logical["name"]) is not None
        if action == "rename":
            return _get(self.value, logical["new"]) is not None
        if action == "value":
            table = _get(self.value, logical["name"])
            return (isinstance(table, dict) and logical["key"] in table
                    and table[logical["key"]] != logical["original_value"])
        return True

    def undo(self, change: dict[str, Any]) -> Edit:
        logical = change["logical"]
        piece, original = change["piece"], change.get("original", "")
        if change["op"] == "create" and self.text == piece:
            return Edit(self.path, "delete", 0, piece, "", self.loaded, None, logical)
        for start in _occurrences(self.text, piece, change.get("offset")):
            after = _splice(self.text, start, piece, original)
            try:
                value = tomllib.loads(after)
            except tomllib.TOMLDecodeError:
                continue
            if logical["action"] == "tables":
                expected = _subtract(self.value, tomllib.loads(logical["core"]))
            elif logical["action"] == "value":
                expected = copy.deepcopy(self.value)
                table = _get(expected, logical["name"])
                if not isinstance(table, dict) or logical["key"] not in table:
                    continue
                table[logical["key"]] = logical["original_value"]
            elif logical["action"] == "rename":
                expected = copy.deepcopy(self.value)
                new, old = logical["new"], logical["name"]
                parent = _get(expected, new[:-1]) if len(new) > 1 else expected
                if not isinstance(parent, dict) or new[-1] not in parent:
                    continue
                table = parent.pop(new[-1])
                target = _get(expected, old[:-1]) if len(old) > 1 else expected
                target[old[-1]] = table
            else:
                expected = copy.deepcopy(self.value)
                table = _get(expected, logical["name"])
                if not isinstance(table, dict):
                    continue
                for key in [key for key, item in table.items() if not isinstance(item, dict)]:
                    del table[key]
                table.update(copy.deepcopy(logical["original_table"]))
            if expected is not None and _prune(value) == _prune(expected):
                return Edit(self.path, "restore" if original else "remove", start, piece, original,
                            self.loaded, after, logical)
        raise EditRefused(self.path, "the entry Chronicle wrote is no longer there as written", _manual_undo(logical))


def _table_names(document: dict[str, Any], prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    names: list[tuple[str, ...]] = []
    for key, value in document.items():
        if isinstance(value, dict):
            names.append((*prefix, key))
            names.extend(_table_names(value, (*prefix, key)))
    return names


# ---------------------------------------------------------------------------
# Plain text: a block between markers
# ---------------------------------------------------------------------------


class TextDoc:
    """A text file (an agent's instruction file) that gets one block between two marker lines."""

    def __init__(self, loaded: Loaded) -> None:
        self.loaded = loaded
        self.path = loaded.path
        self.text = loaded.text

    def find_block(self, begin: str, end: str) -> tuple[int, int] | None:
        """The span of the block from the line starting with *begin* to the line starting with *end*."""
        lines = self.text.split("\n")
        position, opened = 0, None
        for line in lines:
            line_end = position + len(line)
            if line.startswith(begin) and opened is None:
                opened = position
            elif line.startswith(end) and opened is not None:
                return opened, min(line_end + 1, len(self.text))
            position = line_end + 1
        return None

    def append_block(self, block: str, *, logical: dict[str, Any] | None = None) -> Edit:
        text = self.text
        if not text:
            separator = ""
        elif text.endswith("\n\n"):
            separator = ""
        elif text.endswith("\n"):
            separator = "\n"
        else:
            separator = "\n\n"
        piece = separator + block
        logical = {"format": "text", "action": "block", **(logical or {})}
        return Edit(self.path, "create" if not self.loaded.exists else "insert", len(text), "", piece,
                    self.loaded, text + piece, logical)

    def replace_block(self, span: tuple[int, int], block: str, *, logical: dict[str, Any] | None = None) -> Edit:
        start, stop = span
        old = self.text[start:stop]
        new = block if old.endswith("\n") else block.rstrip("\n")
        logical = {"format": "text", "action": "block", **(logical or {})}
        return Edit(self.path, "replace", start, old, new, self.loaded, _splice(self.text, start, old, new), logical)

    def has_trace(self, logical: dict[str, Any]) -> bool:
        """Whether a line of the block's markers is still in the file."""
        markers = [marker for marker in (logical.get("begin"), logical.get("end")) if marker]
        return any(line.startswith(marker) for line in self.text.split("\n") for marker in markers)

    def undo(self, change: dict[str, Any]) -> Edit:
        logical = change["logical"]
        piece, original = change["piece"], change.get("original", "")
        if change["op"] == "create" and self.text == piece:
            return Edit(self.path, "delete", 0, piece, "", self.loaded, None, logical)
        found = _occurrences(self.text, piece, change.get("offset"))
        if len(found) >= 1:
            start = found[0]
            return Edit(self.path, "restore" if original else "remove", start, piece, original, self.loaded,
                        _splice(self.text, start, piece, original), logical)
        span = self.find_block(logical["begin"], logical["end"])
        if span is None:
            raise EditRefused(self.path, "the block Chronicle wrote is not there", _manual_undo(logical))
        start, stop = span
        if original:  # a block that replaced an older one: put that one back
            old = self.text[start:stop]
            return Edit(self.path, "restore", start, old, original, self.loaded,
                        _splice(self.text, start, old, original), logical)
        separator = logical.get("separator", change.get("separator", ""))
        if separator and self.text[start - len(separator):start] == separator:
            start -= len(separator)
        old = self.text[start:stop]
        return Edit(self.path, "remove", start, old, "", self.loaded, _splice(self.text, start, old, ""), logical)


# ---------------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------------


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LedgerBusy(Exception):
    """Another run held the ledger's lock for the whole wait."""

    def __init__(self, path: Path, waited_s: float) -> None:
        super().__init__(f"another chronicle connect or disconnect run holds the lock {path} "
                         f"(waited {waited_s:g} s)")
        self.path = path
        self.waited_s = waited_s


def ledger_lock_path(ledger: Path) -> Path:
    return Path(ledger).with_suffix(LEDGER_LOCK_SUFFIX)


@contextmanager
def ledger_lock(ledger: Path, *, wait_s: float | None = None) -> Iterator[Path]:
    """Hold the ledger's lock file exclusively (``flock``) for one whole run.

    A second run waits up to *wait_s* (``LEDGER_LOCK_WAIT_S``) and then raises
    ``LedgerBusy``. The lock goes with the file descriptor: a run that dies
    leaves no lock behind.
    """
    import fcntl  # POSIX only; connect is refused on Windows before it gets here

    wait_s = LEDGER_LOCK_WAIT_S if wait_s is None else wait_s
    path = ledger_lock_path(ledger)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, LEDGER_MODE)
    try:
        deadline = time.monotonic() + wait_s
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise LedgerBusy(path, wait_s) from None
                time.sleep(LEDGER_LOCK_POLL_S)
        yield path
    finally:
        os.close(descriptor)  # releases the lock


class Ledger:
    """What `connect` wrote, in the workspace: every change before it is made, and its outcome.

    A change is ``pending`` before the write, then ``applied`` (or ``failed``),
    and ``reverted`` once `disconnect` took it back. ``connections`` holds each
    agent's role, address and parts.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.data: dict[str, Any] = {"version": LEDGER_VERSION, "connections": {}, "changes": []}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise EditRefused(self.path, f"the ledger cannot be read: {exc}",
                                  "Move the ledger aside; connect starts a new one.") from exc
            if not isinstance(loaded, dict) or loaded.get("version") != LEDGER_VERSION:
                raise EditRefused(self.path, "the ledger has an unknown format", "Move the ledger aside.")
            self.data = loaded
            self.data.setdefault("connections", {})
            self.data.setdefault("changes", [])

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, temp = tempfile.mkstemp(prefix=".connect-ledger.", dir=self.path.parent)
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(self.data, stream, ensure_ascii=False, indent=1)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temp, LEDGER_MODE)
            os.replace(temp, self.path)
        except BaseException:
            Path(temp).unlink(missing_ok=True)
            raise

    @property
    def changes(self) -> list[dict[str, Any]]:
        return self.data["changes"]

    @property
    def connections(self) -> dict[str, Any]:
        return self.data["connections"]

    def active(self, agent: str, part: str | None = None) -> list[dict[str, Any]]:
        """Applied (and adopted) changes of *agent*, oldest first."""
        return [change for change in self.changes
                if change["agent"] == agent and change["status"] in ("applied", "adopted")
                and (part is None or change["part"] == part)]

    def pending(self) -> list[dict[str, Any]]:
        return [change for change in self.changes if change["status"] == "pending"]

    def begin(self, edit: Edit, *, agent: str, part: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Record *edit* as pending and save, before the file is touched."""
        change = {
            "id": f"c-{uuid.uuid4().hex[:12]}",
            "agent": agent,
            "part": part,
            "file": str(edit.path),
            "op": edit.op,
            "offset": edit.start,
            "piece": edit.new,
            "original": edit.old,
            "logical": edit.logical,
            "sha256_before": edit.before.sha256,
            "sha256_after": edit.sha256_after,
            "backup": None,
            "status": "pending",
            "at": utc_now(),
            **(extra or {}),
        }
        self.changes.append(change)
        self.save()
        return change

    def adopt(self, *, agent: str, part: str, path: Path, text: str, logical: dict[str, Any],
              extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Record an entry that was there already and is taken as Chronicle's own (nothing written)."""
        digest = sha256_file(path)
        change = {
            "id": f"c-{uuid.uuid4().hex[:12]}", "agent": agent, "part": part, "file": str(path), "op": "adopt",
            "offset": None, "piece": text, "original": "", "logical": logical,
            "sha256_before": digest, "sha256_after": digest, "backup": None, "status": "adopted",
            "at": utc_now(), **(extra or {}),
        }
        self.changes.append(change)
        self.save()
        return change

    def finish(self, change: dict[str, Any], result: WriteResult | None, *, status: str = "applied",
               error: str | None = None) -> None:
        change["status"] = status
        if result is not None:
            change["sha256_after"] = result.sha256_after
            change["backup"] = result.backup
        if error:
            change["error"] = error
        change["finished_at"] = utc_now()
        self.save()

    def revert(self, change: dict[str, Any], result: WriteResult | None, *, by: str | None = None) -> None:
        change["status"] = "reverted"
        change["reverted_at"] = utc_now()
        if result is not None:
            change["revert_backup"] = result.backup
            change["sha256_reverted"] = result.sha256_after
        if by:
            change["reverted_by"] = by
        self.save()
