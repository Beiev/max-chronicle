"""Light actions on the memory, taken in the Atlas: pin to the brief, mark stale, hide a copy.

Each action is an event by the operator (category ``curation``, written by
the Atlas), so the history says who did what and when, and undoing one is
one more event. What holds now is the latest action on each memory:
``marks`` folds them in the order they were recorded.

- A **pinned** memory opens the brief of its project, or every brief when it
  has none.
- A **stale** memory stays in recall, marked as such and ranked lower, and
  leaves the brief.
- A **copy**, a memory that says what another one of its kind says (at
  ``DUPLICATE_MIN`` or more by meaning), is hidden. An event is hidden as a
  quarantined one is, by its visibility, so every path that reads events
  leaves it out; a note, a file Chronicle only indexes, is left out where
  notes are read. The history keeps it, and restoring it brings it back.

Only the Atlas's own events by the operator count: an agent can record the
category, not the action.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
import json
import re
import sqlite3
from typing import Any

from .store import (
    ChronicleConfig,
    config_from_manifest,
    guard_visibility,
    open_connection,
    set_event_memory_guard,
    utc_now,
)

CATEGORY = "curation"
SOURCE_KIND = "chronicle_atlas"
OPERATOR = "operator"
# Above this, two memories say the same thing (the Atlas offers them as duplicates).
DUPLICATE_MIN = 0.995
# A stale memory's relevance counts this much in recall: still found, behind current ones.
STALE_WEIGHT = 0.5
# action: (mark, set or cleared)
ACTIONS = {
    "pin": ("pinned", True),
    "unpin": ("pinned", False),
    "stale": ("stale", True),
    "fresh": ("stale", False),
    "duplicate": ("copy", True),
    "restore": ("copy", False),
}
_TEXTS = {
    "pin": "Atlas: {target} pinned to the brief.",
    "unpin": "Atlas: {target} unpinned from the brief.",
    "stale": "Atlas: {target} marked stale.",
    "fresh": "Atlas: {target} marked current again.",
    "duplicate": "Atlas: {target} hidden as a copy of {of}.",
    "restore": "Atlas: {target} restored after being hidden as a copy.",
}
GUARD_SOURCE = "operator_duplicate"
_TARGET = re.compile(r"(event|note):([A-Za-z0-9][A-Za-z0-9_.-]{0,127})")
# In the order the events were written: a clock set back must not reorder two actions.
_FOLD = f"""SELECT rowid AS seq, id, recorded_at_utc, actor, json_extract(payload_json, '$.curation') AS curation
    FROM events WHERE category = '{CATEGORY}' AND source_kind = '{SOURCE_KIND}' AND actor = '{OPERATOR}'
      AND visibility != 'raw_only'
    ORDER BY rowid"""


@dataclass(frozen=True)
class Mark:
    """One mark on a memory: the operator's event that set it, and for a copy what it repeats.

    ``seq`` orders marks set within one second (timestamps have no finer resolution).
    """

    since: str
    by: str
    event_id: str
    of: str | None = None
    seq: int = 0

    def public(self) -> dict[str, Any]:
        view = {"since": self.since, "by": self.by, "event": f"event:{self.event_id}"}
        if self.of:
            view["of"] = self.of
        return view


@dataclass
class Marks:
    """The marks that hold now, by target (``event:<id>`` or ``note:<id>``)."""

    pinned: dict[str, Mark] = field(default_factory=dict)
    stale: dict[str, Mark] = field(default_factory=dict)
    copies: dict[str, Mark] = field(default_factory=dict)

    def table(self, mark: str) -> dict[str, Mark]:
        return {"pinned": self.pinned, "stale": self.stale, "copy": self.copies}[mark]

    def hidden_notes(self) -> set[str]:
        """The ids of notes hidden as copies."""
        return {target[5:] for target in self.copies if target.startswith("note:")}

    def labels(self, target: str) -> dict[str, Any]:
        """What an agent is told about *target*: ``pinned`` and ``stale``, when they hold."""
        labels = {}
        if target in self.pinned:
            labels["pinned"] = self.pinned[target].public()
        if target in self.stale:
            labels["stale"] = self.stale[target].public()
        return labels

    def copies_of(self, original: str) -> list[tuple[str, Mark]]:
        """The copies hidden as repeating *original*, latest first."""
        found = [(copy, mark) for copy, mark in self.copies.items() if mark.of == original]
        return sorted(found, key=lambda item: item[1].seq, reverse=True)


def parse_target(value: Any) -> str:
    """``event:<id>`` or ``note:<id>``; anything else is a ValueError."""
    if not isinstance(value, str) or not _TARGET.fullmatch(value.strip()):
        raise ValueError("a memory is event:<id> or note:<id>")
    return value.strip()


def marks(connection: sqlite3.Connection, *, without: str | None = None) -> Marks:
    """The marks that hold now, from the Atlas's events in the order they were written.

    A quarantined event of the Atlas does not count, nor one whose action is
    malformed; nor the event *without* (one being written).
    """
    current = Marks()
    for row in connection.execute(_FOLD):
        if row["id"] == without:
            continue
        try:
            action = json.loads(row["curation"] or "null")
        except ValueError:
            continue
        if not isinstance(action, dict) or action.get("action") not in ACTIONS:
            continue
        target, of = action.get("target"), action.get("of")
        if not isinstance(target, str) or not _TARGET.fullmatch(target):
            continue
        mark, on = ACTIONS[action["action"]]
        table = current.table(mark)
        if not on:
            table.pop(target, None)
        elif mark != "copy":
            table[target] = Mark(row["recorded_at_utc"], row["actor"], row["id"], seq=row["seq"])
        elif isinstance(of, str) and _TARGET.fullmatch(of):
            table[target] = Mark(row["recorded_at_utc"], row["actor"], row["id"], of, seq=row["seq"])
    return current


def read_marks(config: ChronicleConfig) -> Marks:
    with open_connection(config) as connection:
        return marks(connection)


def annotate(entries: Iterable[dict[str, Any]], current: Marks, *, kind: str = "event",
             key: str = "id") -> None:
    """Add ``pinned`` and ``stale`` to each entry whose memory carries them, in place."""
    for entry in entries:
        identifier = str(entry.get(key) or "")
        target = identifier if identifier.startswith(f"{kind}:") else f"{kind}:{identifier}"
        entry.update(current.labels(target))


@dataclass(frozen=True)
class _Memory:
    target: str
    project: str | None
    domain: str
    shown: bool


def _memory(connection: sqlite3.Connection, config: ChronicleConfig, target: str, current: Marks) -> _Memory | None:
    """The memory *target* names, and whether the map shows it; None when there is none."""
    kind, _, key = target.partition(":")
    if kind == "event":
        row = connection.execute(
            """SELECT e.project, e.domain, e.visibility, e.category,
                      EXISTS (SELECT 1 FROM event_observations o WHERE o.event_id = e.id
                              AND o.visibility != 'raw_only') AS observed
            FROM events e WHERE e.id = ?""", (key,)).fetchone()
        if row is None or row["category"] == CATEGORY:  # the Atlas's own events take no action
            return None
        shown = row["visibility"] != "raw_only" and bool(row["observed"])
        return _Memory(target, config.identities.project(row["project"]), row["domain"] or "global", shown)
    row = connection.execute("SELECT project FROM documents WHERE id = ? AND deleted_at_utc IS NULL",
                             (key,)).fetchone()
    if row is None:
        return None
    return _Memory(target, config.identities.project(row["project"]), "global", target not in current.copies)


def _hide_event(connection: sqlite3.Connection, event_id: str, original: str) -> None:
    from .service import MEMORY_GUARD_VERSION  # noqa: PLC0415 - service writes the event this runs within

    row = connection.execute("SELECT visibility, json_extract(payload_json, '$.memory_guard') AS guard "
                             "FROM events WHERE id = ?", (event_id,)).fetchone()
    if row is None or row["visibility"] == "raw_only":
        raise LookupError(f"event:{event_id} is hidden already")
    set_event_memory_guard(connection, event_id, {
        "version": MEMORY_GUARD_VERSION,
        "verdict": "local_only",
        "visibility": "raw_only",
        "score": 2,
        "signals": [{"name": GUARD_SOURCE, "kind": "risk", "weight": 2}],
        "source": GUARD_SOURCE,
        "duplicate_of": original,
        "reason": f"A copy of {original}, hidden in the Atlas.",
        "previous": json.loads(row["guard"]) if row["guard"] else None,
        "evaluated_at_utc": utc_now(),
    })


def _restore_event(connection: sqlite3.Connection, event_id: str) -> None:
    row = connection.execute("SELECT json_extract(payload_json, '$.memory_guard') AS guard "
                             "FROM events WHERE id = ?", (event_id,)).fetchone()
    guard = json.loads(row["guard"]) if row is not None and row["guard"] else None
    if not isinstance(guard, dict) or guard.get("source") != GUARD_SOURCE:
        # Quarantined since for another reason: that is the operator's CLI to undo, not this.
        raise PermissionError(f"event:{event_id} is hidden for another reason than being a copy")
    previous = guard.get("previous")
    if guard_visibility(previous) == "raw_only":
        previous = None  # a copy was hidden only while it was shown
    set_event_memory_guard(connection, event_id, previous)


def apply_action(
    manifest: dict[str, Any],
    *,
    action: str,
    target: str,
    of: str | None = None,
    request_id: str | None = None,
    defer_embedding: bool = False,
) -> dict[str, Any]:
    """Take *action* on *target* (``of``: the memory a copy repeats) as an event by the operator.

    An action that changes nothing writes nothing. A ValueError is a malformed
    request, a LookupError a memory that is not there (or not shown), a
    PermissionError one that may not be changed this way.
    """
    from . import atlas  # noqa: PLC0415 - the map imports this module
    from .embeddings import active_profile  # noqa: PLC0415
    from .service import record_event  # noqa: PLC0415

    if action not in ACTIONS:
        raise ValueError(f"action must be one of {', '.join(ACTIONS)}")
    target = parse_target(target)
    mark, on = ACTIONS[action]
    if mark == "copy" and on:
        of = parse_target(of)
        if of == target or of.partition(":")[0] != target.partition(":")[0]:
            raise ValueError("a copy repeats another memory of its own kind")
    else:
        of = None
    config = config_from_manifest(manifest)
    if config.identities.agent(OPERATOR) != OPERATOR:
        raise PermissionError(
            f"the manifest makes {OPERATOR!r} a spelling of {config.identities.agent(OPERATOR)!r}: "
            f"remove it from that agent's aliases so the operator's actions are the operator's")
    with open_connection(config) as connection:
        current = marks(connection)
        subject = _memory(connection, config, target, current)
        held = target in current.table(mark)
        if subject is None or (not subject.shown and not (mark == "copy" and held)):
            raise LookupError(f"{target} is not on the map")
        if held == on:
            return {"status": "unchanged", "target": target, "marks": current.labels(target)}
        if of is not None:
            original = _memory(connection, config, of, current)
            if original is None or not original.shown:
                raise LookupError(f"{of} is not on the map")
            similarity = atlas.memory_similarity(connection, active_profile().key, target, of)
            if similarity is None or similarity < DUPLICATE_MIN:
                raise PermissionError(f"{target} and {of} do not say the same thing"
                                      + (f" (similarity {similarity:.3f})" if similarity is not None else
                                         " (no vectors to compare)"))

    kind, _, key = target.partition(":")

    def within(connection: sqlite3.Connection, stored: dict[str, Any]) -> None:
        if mark != "copy":
            return
        # Again in the write transaction, as before this action: another writer
        # may have hidden either memory since (each as a copy of the other would
        # hide both).
        now = marks(connection, without=stored["id"])
        if on:
            again = _memory(connection, config, target, now), _memory(connection, config, of or "", now)
            if not all(memory is not None and memory.shown for memory in again):
                raise LookupError(f"{target} or {of} is no longer on the map")
        elif target not in now.copies:
            raise LookupError(f"{target} is not hidden as a copy")
        if kind == "event":
            if on:
                _hide_event(connection, key, of or "")
            else:
                _restore_event(connection, key)

    stored = record_event(
        manifest,
        {
            "agent": OPERATOR,
            "agent_source": "explicit",
            "category": CATEGORY,
            "domain": subject.domain,
            "project": subject.project,
            "text": _TEXTS[action].format(target=target, of=of),
            "request_id": request_id,
            "curation": {"action": action, "target": target, "of": of},
        },
        dedupe=False,
        hash_dedupe=False,  # pin, unpin, pin again: three events, not one
        source_kind=SOURCE_KIND,
        imported_from="max_chronicle.atlas.action",
        defer_embedding=defer_embedding,
        within=within,
    )
    with open_connection(config) as connection:
        after = marks(connection)
    return {"status": "done", "target": target, "event_id": stored["id"],
            "replayed": stored.get("chronicle_status") != "stored", "marks": after.labels(target),
            "hidden": target in after.copies}
