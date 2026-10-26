"""What an agent receives: short recall hits first, full records on request.

Recall used to return every hit in full, with provenance, so a dozen hits
cost an agent thousands of tokens it mostly did not read. ``index_view``
turns a recall response into one line per hit — id, when, where, who, a
gist of the text, how it matched — and ``get_items`` returns the full
records for the ids an agent picks. Each hit says how much stands behind
it (``trust``): archived evidence, confirmation by more than one agent, a
single agent's assertion, or an assumption. Ids carry their kind: ``event:<id>``,
``note:<id>`` and ``fact:<id>`` (a brief prints fact ids); a bare id is an
event, or else a fact.
"""

from __future__ import annotations

from typing import Any

from .memory import FACT_SCOPE, event_provenance
from .notes import read_note
from .store import config_from_manifest, fetch_event, open_connection

GIST_CHARS = 280
WHY_GIST_CHARS = 160
MAX_GET_IDS = 20
INDEX_NEXT = "get_memory(ids=[...]) returns the full text, evidence and provenance of chosen hits."


def gist(text: str | None, limit: int = GIST_CHARS) -> tuple[str | None, bool]:
    """*text* on one line, cut to *limit* characters; and whether it was cut."""
    if not text:
        return None, False
    flat = " ".join(text.split())
    if len(flat) <= limit:
        return flat, False
    return flat[: limit - 1].rstrip() + "…", True


def _match(channels: dict[str, Any]) -> dict[str, Any]:
    similarity = channels.get("vector_similarity")
    return {"words": channels.get("fts_rank") is not None,
            "similarity": round(similarity, 3) if similarity is not None else None}


def trust(hit: dict[str, Any]) -> str:
    """What stands behind a recalled event, from its provenance.

    ``assumption``: its author marked the fact it set as an assumption; nothing
    attached outranks that. Otherwise, strongest first: ``evidence``, a copy of
    a source file was archived when it was recorded (the file backs the record;
    it does not prove the text); ``confirmed``, more than one agent recorded the
    same text; ``assertion``, one agent's word. A poisoned or mistaken record
    most often looks like the last.
    """
    provenance = hit.get("provenance") or {}
    observations = provenance.get("observations") or []
    summary = provenance.get("summary") or {}
    if any(fact.get("kind") == "assumption" for fact in provenance.get("facts") or []):
        return "assumption"
    archived = summary.get("archived_evidence")
    if archived is None:  # a provenance from before the summary
        archived = any((item or {}).get("status") == "archived" for observation in observations
                       for item in observation.get("evidence") or [])
    if archived:
        return "evidence"
    agents = summary.get("agents")
    if agents is None:
        agents = len({observation.get("agent") for observation in observations if observation.get("agent")})
    return "confirmed" if agents > 1 else "assertion"


def _event_hit(hit: dict[str, Any]) -> dict[str, Any]:
    text, cut = gist(hit.get("text"))
    why, why_cut = gist(hit.get("why"), WHY_GIST_CHARS)
    item = {
        "id": f"event:{hit['event_id']}",
        "when": hit.get("occurred_at_utc"),
        "project": hit.get("project"),
        "task_id": hit.get("task_id"),
        "agent": hit.get("agent"),
        "category": hit.get("category"),
        "text": text,
        "match": _match(hit.get("channels") or {}),
        "trust": trust(hit),
    }
    if why:
        item["why"] = why
    if cut or why_cut:
        item["truncated"] = True
    evidence = len(hit.get("source_files") or [])
    if evidence:
        item["evidence_files"] = evidence
    return item


def _note_hit(note: dict[str, Any]) -> dict[str, Any]:
    text, cut = gist(note.get("text"))
    item = {
        "id": f"note:{note['document_id']}",
        "title": note.get("title"),
        "section": note.get("heading"),
        "project": note.get("project"),
        "modified": note.get("modified_at_utc"),
        "text": text,
        "match": _match(note.get("channels") or {}),
        "trust": "note",  # a file someone keeps on purpose; its author is the file's
    }
    if cut or (note.get("text") or "").endswith("…"):
        item["truncated"] = True
    return item


def index_view(response: dict[str, Any]) -> dict[str, Any]:
    """The recall *response* with one short line per hit instead of full records."""
    view = {key: response[key] for key in ("query", "project", "task_id", "domain") if response.get(key) is not None}
    view["results"] = [_event_hit(hit) for hit in response.get("results", [])]
    if "notes" in response:
        view["notes"] = [_note_hit(note) for note in response["notes"]]
    for key in ("no_confident_match", "relaxed", "degraded"):
        view[key] = bool(response.get(key))
    if response.get("channel_errors"):
        view["channel_errors"] = response["channel_errors"]
    coverage = response.get("vector_coverage") or {}
    if coverage.get("missing_or_incompatible"):
        # Not degraded, but meaning-search saw only part of the memory (a backfill
        # under way, a model change half re-embedded).
        view["vector_coverage"] = {key: coverage.get(key) for key in ("eligible", "missing_or_incompatible")}
    if response.get("hint"):
        view["hint"] = response["hint"]
    if view["results"] or view.get("notes"):
        view["next"] = INDEX_NEXT
    return view


def _visible_event(config, event_id: str) -> dict[str, Any] | None:
    event = fetch_event(config, event_id=event_id)
    if event is None or event["visibility"] == "raw_only":
        return None  # quarantined events stay out of agents' reach
    return event


def _event_item(config, event_id: str) -> dict[str, Any] | None:
    event = _visible_event(config, event_id)
    if event is None:
        return None
    return {**event, "id": f"event:{event_id}", "kind": "event", "provenance": event_provenance(config, event_id)}


def _fact_item(config, fact_id: str) -> dict[str, Any] | None:
    """A fact with the event that asserted it; a fact whose event is hidden is hidden too."""
    with open_connection(config) as connection:
        row = connection.execute(
            """SELECT id, slot_key, value_key, fact_text, status, recorded_at_utc, expired_at_utc,
                      attributed_to, """ + ", ".join(FACT_SCOPE) + """
            FROM facts WHERE id = ?""", (fact_id,)).fetchone()
    if row is None:
        return None
    attributes = {key: row[key] for key in FACT_SCOPE}
    event_id = attributes["event_id"]
    event = _visible_event(config, event_id) if event_id else None
    if event is None:
        return None
    return {
        "id": f"fact:{fact_id}", "kind": "fact", "slot": row["slot_key"], "value": row["value_key"],
        "text": row["fact_text"], "agent": row["attributed_to"], "recorded_at_utc": row["recorded_at_utc"],
        "current": row["status"] == "active" and row["expired_at_utc"] is None,
        "attributes": attributes, "event": {**event, "id": f"event:{event_id}"},
    }


def get_items(manifest: dict[str, Any], ids: list[str]) -> dict[str, Any]:
    """Full records for recall ids, in the order asked, each once; ids that name nothing are listed apart."""
    if not ids:
        raise ValueError("ids must name at least one event:, note: or fact: id")
    if len(ids) > MAX_GET_IDS:
        raise ValueError(f"at most {MAX_GET_IDS} ids per call")
    config = config_from_manifest(manifest)
    items: list[dict[str, Any]] = []
    missing: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        kind, separator, identifier = raw.strip().partition(":")
        kind, identifier = (kind.strip().lower(), identifier.strip()) if separator else ("", kind.strip())
        item = None
        if identifier and kind == "note":
            note = read_note(manifest, identifier)
            item = {**note, "id": f"note:{identifier}", "kind": "note"} if note is not None else None
        elif identifier and kind in ("event", ""):
            item = _event_item(config, identifier)
            if item is None and not kind:
                item = _fact_item(config, identifier)
        elif identifier and kind == "fact":
            item = _fact_item(config, identifier)
        if item is None:
            missing.append(raw)
        elif item["id"] not in seen:  # the same record under two spellings comes back once
            seen.add(item["id"])
            items.append(item)
    return {"items": items, "missing": missing}
