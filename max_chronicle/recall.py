"""Scoped hybrid recall. Recent activity alone is never search evidence."""

from __future__ import annotations

import math
import os
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from .notes import recall_notes
from .store import (
    _parse_iso,
    config_from_manifest,
    fetch_event,
    fetch_recall_pool,
    search_events,
    search_fact_events,
    strict_matches,
)

NOTE_LIMIT = 5  # notes per recall, each its best section
ENV_MIN_SIMILARITY = "CHRONICLE_VECTOR_MIN_SIMILARITY"
ENV_CONFIDENT_SIMILARITY = "CHRONICLE_VECTOR_CONFIDENT_SIMILARITY"
NO_CONFIDENT_MATCH_HINT = (
    "No result is a confident match: treat these results as leads and verify them, or "
    "rephrase the query. A weak or empty result does not prove the memory is absent."
)


def _similarity_setting(name: str, default: float | None) -> float | None:
    """A cosine setting from the environment, or the profile's default."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    value = float(raw)
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return value


def query_memory(
    manifest: dict[str, Any],
    *,
    query: str,
    domain: str | None = None,
    limit: int = 10,
    project: str | None = None,
    task_id: str | None = None,
    include_notes: bool = True,
) -> dict[str, Any]:
    """Fuse relevant lexical/vector candidates; recency only breaks ties.

    Events come back in ``results``. With ``include_notes``, indexed notes
    (FR-10) come back in ``notes``, best section per note: they have no task
    or domain, and in a project scope, global notes follow the project's.

    RRF scores are ranking signals, not confidence. A cosine floor (the active
    embedding profile's, or CHRONICLE_VECTOR_MIN_SIMILARITY) rejects weak
    vector-only candidates; exact lexical hits need no embedding. The response
    says ``no_confident_match`` unless a result holds every query term or
    reaches the profile's confident similarity (FR-2).
    """
    from .embeddings import active_profile, embed_query, similarity_scorer, unpack_vector
    from .memory import event_provenance

    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    if isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if task_id and not project:
        raise ValueError("task_id requires project")
    profile = active_profile()
    threshold = _similarity_setting(ENV_MIN_SIMILARITY, profile.min_similarity)
    confident_floor = _similarity_setting(ENV_CONFIDENT_SIMILARITY, profile.confident_similarity)
    config = config_from_manifest(manifest)
    scope = dict(domain=domain, project=project, task_id=task_id)
    pool = fetch_recall_pool(config, model_key=profile.key, **scope)
    eligible = {row["id"]: row for row in pool}
    errors: dict[str, str] = {}
    fts: dict[str, int] = {}
    fact_matches: set[str] = set()
    relaxed = False
    try:
        wanted = max(limit * 4, 40)
        hits = search_events(config, query=query, limit=wanted, current_only=True, **scope)
        fact_hits = search_fact_events(config, query=query, limit=wanted, **scope)
        if not any(hit["id"] in eligible for hit in hits) and not any(e in eligible for e in fact_hits):
            # No event or fact in scope holds every term: accept events whose
            # stems cover most of them, and say so, since such a match is
            # weaker (FR-2). A hit outside the pool, such as a fact recorded
            # after the pool was read, is not evidence here either.
            hits = search_events(config, query=query, limit=wanted, current_only=True, relaxed=True, **scope)
            relaxed = any(hit["id"] in eligible for hit in hits)
        fts = {
            hit["id"]: rank for rank, hit in enumerate(hits) if hit["id"] in eligible
        }
        for rank, event_id in enumerate(fact_hits):
            if event_id in eligible:
                fts[event_id] = min(fts.get(event_id, rank), rank)
                fact_matches.add(event_id)
    except Exception as exc:
        errors["fts"] = f"{type(exc).__name__}: {exc}"

    compatible = [r for r in pool if r["vector"] is not None and len(r["vector"]) == r["dim"] * 4]
    similarities: dict[str, float] = {}
    vector_available = False
    usable_query_vec: list[float] | None = None
    try:
        query_vec = embed_query(query)
        if query_vec is None:
            errors["vector"] = "embedding_backend_unavailable"
        elif not all(math.isfinite(x) for x in query_vec) or not any(query_vec):
            errors["vector"] = "invalid_query_embedding"
        else:
            usable_query_vec = query_vec
            # A vector of another dimension (the model behind the key changed)
            # is incompatible: coverage counts it and embed-backfill replaces it.
            compatible = [r for r in compatible if r["dim"] == len(query_vec)]
            if pool and not compatible:
                # No usable index yet (run embed-backfill). A partial index
                # still ranks what it holds, and coverage discloses the rest.
                errors["vector"] = "embedding_index_empty"
            vector_available = bool(compatible)
            score = similarity_scorer(query_vec)
            for row in compatible:
                try:
                    vec = unpack_vector(row["vector"])
                    if not any(vec):
                        raise ValueError("zero vector")
                    similarities[row["id"]] = score(vec)
                except (ValueError, TypeError) as exc:
                    errors["vector"] = f"invalid_stored_embedding: {exc}"
    except Exception as exc:
        errors["vector"] = f"{type(exc).__name__}: {exc}"
    coverage = {
        "model_key": profile.key,
        "eligible": len(pool),
        "compatible": len(compatible),
        "missing_or_incompatible": len(pool) - len(compatible),
    }

    # Only a similarity above the floor makes a vector candidate; every one is
    # still reported, and counts toward confidence. Equal similarity (such as
    # the same text recorded twice) ranks the newer event first.
    admitted = [eid for eid, similarity in similarities.items() if similarity >= threshold]
    newest_first = sorted(admitted, key=lambda eid: (eligible[eid]["occurred_at_utc"], eid), reverse=True)
    vector_ids = sorted(newest_first, key=lambda eid: -similarities[eid])[: max(limit * 4, 40)]
    vector_ranks = {eid: rank for rank, eid in enumerate(vector_ids)}
    candidates = set(fts) | set(vector_ranks)
    recent_ids = sorted(
        candidates,
        key=lambda eid: (eligible[eid]["occurred_at_utc"], eid),
        reverse=True,
    )
    recency = {eid: rank for rank, eid in enumerate(recent_ids)}
    # Relevance alone scores a candidate; recency only breaks ties (FR-1).
    scores = {
        eid: sum(1 / (60 + rank) for rank in (fts.get(eid), vector_ranks.get(eid)) if rank is not None)
        for eid in candidates
    }
    ranked = sorted(candidates, key=lambda eid: (-scores[eid], recency[eid], eid))[
        :limit
    ]
    results = []
    for eid in ranked:
        event = fetch_event(config, event_id=eid)
        if event is None:
            continue
        occurred = event.get("recorded_at") or ""
        local = (
            _parse_iso(occurred)
            .astimezone(ZoneInfo(manifest.get("settings", {}).get("timezone", "UTC")))
            .isoformat(timespec="seconds")
        )
        results.append(
            {
                "event_id": eid,
                "text": event["text"],
                "why": event.get("why"),
                "category": event.get("category"),
                "project": event.get("project"),
                "domain": event.get("domain"),
                "agent": event.get("agent"),
                "task_id": event.get("task_id"),
                "source_files": event.get("source_files", []),
                "occurred_at_utc": occurred,
                "occurred_at_local": local,
                "rrf_score": round(scores[eid], 6),
                "channels": {
                    "fts_rank": fts.get(eid),
                    "vector_similarity": similarities.get(eid),
                    "recency_rank": recency[eid],
                },
                "provenance": event_provenance(config, eid),
            }
        )
    channels = (
        (["fts"] if fts else [])
        + (["vector"] if vector_available else [])
        + (["temporal"] if candidates else [])
    )
    # Confident evidence holds every query term (a relaxed match does not), or
    # is as close as this model gets only for related text. Every term is
    # checked on the returned events themselves: one ranked below the capped
    # full-text list still holds them.
    returned = [hit["event_id"] for hit in results]
    try:
        lexical = strict_matches(config, query=query, event_ids=returned) | (fact_matches & set(returned))
    except sqlite3.Error:
        lexical = set()  # unknown, so not confident
    confident = any(
        hit["event_id"] in lexical
        or (
            confident_floor is not None
            and hit["channels"]["vector_similarity"] is not None
            and hit["channels"]["vector_similarity"] >= confident_floor
        )
        for hit in results
    )
    notes: list[dict[str, Any]] = []
    if include_notes:
        try:
            found = recall_notes(config, query=query, project=project, query_vector=usable_query_vec,
                                 threshold=threshold, limit=min(limit, NOTE_LIMIT))
            notes = found["notes"]
            # A note holding every query term, or close enough, is confident evidence too.
            confident = confident or any(
                note["chunk_id"] in found["strict"]
                or (confident_floor is not None and note["channels"]["vector_similarity"] is not None
                    and note["channels"]["vector_similarity"] >= confident_floor)
                for note in notes
            )
        except (sqlite3.Error, ValueError) as exc:
            errors["notes"] = f"{type(exc).__name__}: {exc}"
    response = {
        "query": query,
        **scope,
        "results": results,
        **({"notes": notes} if include_notes else {}),
        "relaxed": relaxed,
        "no_confident_match": not confident,
        "channels_used": channels,
        "degraded": bool(errors),
        "channel_errors": errors,
        "vector_coverage": coverage,
        "similarity_floors": {"min": threshold, "confident": confident_floor},
    }
    if not confident:
        response["hint"] = NO_CONFIDENT_MATCH_HINT
    return response
