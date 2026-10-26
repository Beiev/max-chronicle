"""What an agent receives: short recall hits first, full records on request.

Recall used to return every hit in full, with provenance, so a dozen hits
cost an agent thousands of tokens it mostly did not read. ``index_view``
turns a recall response into one line per hit — id, when, where, who, a
gist of the text, how it matched — and ``get_items`` returns the full
records for the ids an agent picks. Ids carry their kind: ``event:<id>`` and
``note:<id>``; a bare id is an event.
"""

from __future__ import annotations

from typing import Any

from .memory import event_provenance
from .notes import read_note
from .store import config_from_manifest, fetch_event

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
    if response.get("hint"):
        view["hint"] = response["hint"]
    if view["results"] or view.get("notes"):
        view["next"] = INDEX_NEXT
    return view


def get_items(manifest: dict[str, Any], ids: list[str]) -> dict[str, Any]:
    """Full records for recall ids, in the order asked; unknown ids are listed apart."""
    if not ids:
        raise ValueError("ids must name at least one event: or note: id")
    if len(ids) > MAX_GET_IDS:
        raise ValueError(f"at most {MAX_GET_IDS} ids per call")
    config = config_from_manifest(manifest)
    items: list[dict[str, Any]] = []
    missing: list[str] = []
    for raw in ids:
        kind, _, identifier = raw.partition(":") if ":" in raw else ("event", "", raw)
        identifier = identifier.strip()
        if kind == "note":
            note = read_note(manifest, identifier)
            if note is None:
                missing.append(raw)
            else:
                items.append({**note, "id": f"note:{identifier}", "kind": "note"})
        elif kind == "event":
            event = fetch_event(config, event_id=identifier)
            if event is None or (event.get("memory_guard") or {}).get("visibility") == "raw_only":
                missing.append(raw)  # quarantined events stay out of agents' reach
            else:
                items.append({**event, "id": f"event:{identifier}", "kind": "event",
                              "provenance": event_provenance(config, identifier)})
        else:
            raise ValueError(f"unknown id kind in {raw!r}: use event:<id> or note:<id>")
    return {"items": items, "missing": missing}
