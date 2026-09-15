"""Scoped hybrid recall. Recent activity alone is never search evidence."""

from __future__ import annotations

import math
import os
from typing import Any
from zoneinfo import ZoneInfo

from .store import (
    _parse_iso,
    config_from_manifest,
    fetch_event,
    fetch_recall_pool,
    search_events,
    search_fact_events,
)


def query_memory(
    manifest: dict[str, Any],
    *,
    query: str,
    domain: str | None = None,
    limit: int = 10,
    project: str | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    """Fuse relevant lexical/vector candidates, then apply a recency prior.

    RRF scores are ranking signals, not confidence. A configurable cosine floor
    rejects weak vector-only candidates; exact lexical hits need no embedding.
    """
    from .embeddings import EMBED_DIM, EMBED_MODEL, cosine, embed_text, unpack_vector
    from .memory import event_provenance

    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    if isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if task_id and not project:
        raise ValueError("task_id requires project")
    threshold = float(os.environ.get("CHRONICLE_VECTOR_MIN_SIMILARITY", "0.65"))
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("CHRONICLE_VECTOR_MIN_SIMILARITY must be between 0 and 1")
    config = config_from_manifest(manifest)
    scope = dict(domain=domain, project=project, task_id=task_id)
    pool = fetch_recall_pool(config, **scope)
    eligible = {row["id"]: row for row in pool}
    errors: dict[str, str] = {}
    fts: dict[str, int] = {}
    try:
        hits = search_events(
            config, query=query, limit=max(limit * 4, 40), current_only=True, **scope
        )
        fts = {
            hit["id"]: rank for rank, hit in enumerate(hits) if hit["id"] in eligible
        }
        for rank, event_id in enumerate(
            search_fact_events(config, query=query, limit=max(limit * 4, 40), **scope)
        ):
            if event_id in eligible:
                fts[event_id] = min(fts.get(event_id, rank), rank)
    except Exception as exc:
        errors["fts"] = f"{type(exc).__name__}: {exc}"

    compatible = [
        r
        for r in pool
        if r["model"] == EMBED_MODEL
        and r["dim"] == EMBED_DIM
        and r["vector"] is not None
        and len(r["vector"]) == EMBED_DIM * 4
    ]
    coverage = {
        "eligible": len(pool),
        "compatible": len(compatible),
        "missing_or_incompatible": len(pool) - len(compatible),
    }
    similarities: dict[str, float] = {}
    vector_available = False
    try:
        query_vec = embed_text(query)
        if query_vec is None:
            errors["vector"] = "embedding_backend_unavailable"
        elif (
            len(query_vec) != EMBED_DIM
            or not all(math.isfinite(x) for x in query_vec)
            or not any(query_vec)
        ):
            errors["vector"] = "invalid_query_embedding"
        else:
            vector_available = bool(compatible)
            for row in compatible:
                try:
                    vec = unpack_vector(row["vector"])
                    if not any(vec):
                        raise ValueError("zero vector")
                    similarity = cosine(query_vec, vec)
                    if similarity >= threshold:
                        similarities[row["id"]] = similarity
                except (ValueError, TypeError) as exc:
                    errors["vector"] = f"invalid_stored_embedding: {exc}"
            if len(compatible) < len(pool):
                errors.setdefault("vector", "embedding_index_incomplete")
    except Exception as exc:
        errors["vector"] = f"{type(exc).__name__}: {exc}"

    vector_ids = sorted(similarities, key=lambda eid: (-similarities[eid], eid))[
        : max(limit * 4, 40)
    ]
    vector_ranks = {eid: rank for rank, eid in enumerate(vector_ids)}
    candidates = set(fts) | set(vector_ranks)
    recent_ids = sorted(
        candidates,
        key=lambda eid: (eligible[eid]["occurred_at_utc"], eid),
        reverse=True,
    )
    recency = {eid: rank for rank, eid in enumerate(recent_ids)}
    scores = {
        eid: sum(
            1 / (60 + rank)
            for rank in (fts.get(eid), vector_ranks.get(eid), recency[eid])
            if rank is not None
        )
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
    return {
        "query": query,
        **scope,
        "results": results,
        "channels_used": channels,
        "degraded": bool(errors),
        "channel_errors": errors,
        "vector_coverage": coverage,
    }
