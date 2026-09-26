from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import unicodedata
from typing import Any

from .config import (
    ACTIVATION_CONTRACT_NAME,
    ACTIVATION_CONTRACT_VERSION,
    DEFAULT_EVENT_DEDUP_WINDOW_HOURS,
    DEFAULT_MEM0_LIVE_TIMEOUT_S,
    ENV_EVENT_DEDUP_WINDOW_HOURS,
    ENV_FEATURE_ENTITY_ALIASES,
    ENV_FEATURE_EVENT_HASH_DEDUP,
    ENV_FEATURE_MEM0_LIVE_SEARCH,
    ENV_MEM0_LIVE_TIMEOUT_S,
    env_float,
    env_int,
    feature_enabled,
)
from .db import database_summary
from .identity import fold
from .recall import query_memory
from .redaction import redact_value
from .projections import render_job_search_status, render_status_generated_block, update_status_file
from .runtime_context import (
    append_jsonl,
    build_runtime_snapshot,
    expand_path,
    file_meta,
    mem0_meta,
    normalize_heading,
    read_json,
    read_jsonl,
    read_source_content,
    render_activation_prompt,
    search_mem0_dump,
    search_source_blocks,
    score_text,
    shorten,
    source_freshness,
    tokenize,
    utc_now,
)
from .store import (
    _atomic_write_text,
    add_entity_alias,
    config_from_manifest,
    DEFAULT_STALE_RUN_TTL_HOURS,
    entity_alias_stats,
    fetch_event,
    fetch_event_ids_without_embedding,
    fetch_latest_projection_run,
    fetch_latest_snapshot,
    fetch_normalized_entities,
    fetch_project_events,
    fetch_recent_events,
    fetch_relations_for_entity,
    find_event_by_content_hash_recent,
    finish_ingest_run,
    link_artifact,
    mark_missing_normalized_entities_inactive,
    mark_stale_running_runs,
    merge_normalized_entities,
    open_connection,
    persist_staged_artifact,
    search_events,
    stage_artifact_from_path,
    start_ingest_run,
    store_artifact_from_path,
    store_event,
    store_event_embedding,
    store_projection_run,
    store_snapshot,
    timeline_state,
    update_event_mem0_state,
    update_event_memory_guard,
    upsert_normalized_entity,
    write_transaction,
)


VALID_CATEGORY_RE = re.compile(r"^[a-z0-9_.-]{2,64}$")
ENV_FEATURE_EVENT_EMBEDDINGS = "CHRONICLE_FEATURE_EVENT_EMBEDDINGS"
VALID_RELATION_RE = re.compile(r"^[a-z_]+$")
FRESHNESS_THRESHOLDS_HOURS = {
    "live": 6,
    "recent": 48,
    "stale": 24 * 7,
}
TRUST_TIER_RANK = {
    "canonical": 4,
    "operator_curated": 3,
    "reference": 2,
    "semantic_recall": 1,
    "unknown": 0,
}
FRESHNESS_RANK = {
    "live": 4,
    "recent": 3,
    "stale": 2,
    "archival": 1,
    "unknown": 0,
}
VALID_TRUST_TIERS = frozenset(TRUST_TIER_RANK)
CANONICAL_EVENT_CATEGORIES = frozenset(
    {
        "note",
        "decision",
        "milestone",
        "implementation",
        "maintenance",
        "anomaly",
        "workflow",
        "daily_summary",
        "digest_run",
        "git_commit",
        "world_event",
        "architecture_insight",
        "blocker",
        "state_change",
        "observation",
        "research",
        "insight",
        "constraint",
    }
)
CATEGORY_ALIASES = {
    "block": "blocker",
    "status_change": "state_change",
}
MCP_MEM0_NOISE_CATEGORIES = frozenset(
    {
        "daily_summary",
        "git_commit",
        "maintenance",
    }
)
MEMORY_GUARD_VERSION = "2026-03-22.v1"
MEMORY_GUARD_SCOPED_CATEGORIES = frozenset({"note", "state_change", "maintenance", "observation"})
MEMORY_GUARD_EVIDENCE_CATEGORIES = frozenset(
    {"decision", "milestone", "blocker", "implementation", "architecture_insight", "workflow", "insight", "constraint"}
)
MEMORY_GUARD_PROTOCOL_TERMS = (
    "agent",
    "session",
    "protocol",
    "prompt",
    "tool update",
    "memory update",
    "memory tool",
    "activation prompt",
    "startup bundle",
    "mcp server",
)
MEMORY_GUARD_NEGATIVE_TERMS = ("do not", "don't", "never", "не ")
MEMORY_GUARD_POSITIVE_TERMS = ("instead", "use ", "replace", "updated", "canonical", "old", "new")
MEMORY_GUARD_ABSOLUTE_TERMS = ("sole", "only supported", "always", "never", "canonical now", "sole interface")
MEMORY_GUARD_VERDICTS = frozenset({"durable", "local_only"})
ISSUE_SEVERITY_RANK = {
    "critical": 2,
    "warn": 1,
}
V2_ATTACH_BUNDLE_VERSION = "memory-v2"
QUERY_MODES = (
    "truth_only",
    "truth_plus_interpretation",
    "truth_plus_interpretation_plus_scenarios",
)
DEFAULT_QUERY_MODE = "truth_plus_interpretation"
LANE_DEFAULTS: dict[str, dict[str, Any]] = {
    "work": {
        "label": "Work",
        "sensitive": False,
        "default_enabled": True,
    },
    "agents": {
        "label": "Agents",
        "sensitive": False,
        "default_enabled": True,
    },
    "world": {
        "label": "World",
        "sensitive": False,
        "default_enabled": True,
    },
    "career_market": {
        "label": "Career Market",
        "sensitive": False,
        "default_enabled": True,
    },
    "companies": {
        "label": "Companies",
        "sensitive": False,
        "default_enabled": True,
    },
    "life_admin": {
        "label": "Life Admin",
        "sensitive": True,
        "default_enabled": False,
    },
    "finances": {
        "label": "Finances",
        "sensitive": True,
        "default_enabled": False,
    },
    "health": {
        "label": "Health",
        "sensitive": True,
        "default_enabled": False,
    },
    "decisions": {
        "label": "Decisions",
        "sensitive": False,
        "default_enabled": True,
    },
    "vectors": {
        "label": "Vectors",
        "sensitive": False,
        "default_enabled": True,
    },
}
ROLE_TO_LANE = {
    "ssot": "work",
    "strategy": "vectors",
    "pipeline_metrics": "career_market",
    "ops": "agents",
    "memory_policy": "decisions",
    "timeline_protocol": "decisions",
    "architecture": "decisions",
    "schema": "decisions",
    "implementation_plan": "decisions",
    "global_context": "vectors",
    "ownership": "decisions",
}
SOURCE_ROLE_QUESTIONS = {
    "ssot": {
        "can": [
            "What is the current operator-facing state?",
            "What changed recently in active work?",
        ],
        "cannot": [
            "What is objectively true in the external world right now without corroboration?",
            "What-if scenarios or speculative forecasts.",
        ],
    },
    "strategy": {
        "can": [
            "What is the current priority stack?",
            "Which constraints and guard rails are active?",
        ],
        "cannot": [
            "Live runtime state or execution telemetry.",
            "External world claims without evidence.",
        ],
    },
    "pipeline_metrics": {
        "can": [
            "What is the current job search pipeline state?",
            "Which leads and counts are active?",
        ],
        "cannot": [
            "Broader labor market reality beyond current pipeline coverage.",
            "Personal life or health state.",
        ],
    },
    "ops": {
        "can": [
            "How should autonomous systems be operated?",
            "What runbook or operational procedure applies?",
        ],
        "cannot": [
            "Canonical project truth when runtime evidence disagrees.",
            "Scenario analysis.",
        ],
    },
    "memory_policy": {
        "can": [
            "What are the current memory rules and trust boundaries?",
            "How should Chronicle and Mem0 be used?",
        ],
        "cannot": [
            "Live system state or latest world events.",
            "Future predictions.",
        ],
    },
}
COMPANY_SUFFIX_TOKENS = {
    "inc",
    "incorporated",
    "llc",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "co",
    "company",
    "gmbh",
    "ag",
    "plc",
    "sa",
}
COMPANY_SOURCE_SUFFIXES = {
    "nodesk",
    "remotive",
    "remotive.com",
    "jobs",
    "job board",
}
COMPANY_SOURCE_PREFIXES = {
    "remote",
}
GENERIC_COMPANY_ALIASES = {
    "actually work for content creators",
    "aiartistjobs",
    "aijobs",
    "built in chicago",
    "built in nyc",
    "business",
    "climatechangecareers",
    "cloud",
    "dynamitejobs",
    "entertainmentcareers",
    "jobright",
    "marketing",
    "marketing and",
    "meetfrank",
    "remote marketing",
    "remotefront",
    "showbizjobs",
    "sr",
}
RUNTIME_SOURCE_DEFAULTS = {
    "portfolio_asset_manifest": {"label": "Portfolio Asset Manifest", "lane": "work"},
    "company_intel_json": {"label": "Company Intel", "lane": "companies"},
}


def _config(manifest: dict[str, Any]):
    return config_from_manifest(manifest)


def _compat_path(manifest: dict[str, Any], key: str) -> Path:
    return expand_path(manifest["paths"][key])


def _operator_label(manifest: dict[str, Any]) -> str:
    """Installation owner shown in agent prompts; generic unless the manifest names one."""
    return str(manifest.get("settings", {}).get("operator") or "the operator")


def _source_path(manifest: dict[str, Any], source_id: str) -> Path:
    return expand_path(manifest["source_map"][source_id]["path"])


def _projection_note(manifest: dict[str, Any], source_id: str) -> str | None:
    """Operator-supplied banner for a generated projection, from the manifest."""
    source = manifest.get("source_map", {}).get(source_id) or {}
    note = source.get("projection_note")
    return str(note).strip() or None if note else None


def default_mem0_status(entry: dict[str, Any], *, source_kind: str) -> str | None:
    explicit = entry.get("mem0_status")
    if explicit not in {None, "", "auto"}:
        return str(explicit)
    if source_kind != "chronicle_mcp":
        return None
    if entry.get("category") in MCP_MEM0_NOISE_CATEGORIES:
        return "off"
    return "queued"


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _validate_category(value: str | None) -> None:
    if not value:
        return
    if not VALID_CATEGORY_RE.fullmatch(value):
        raise ValueError(f"Invalid category: {value}")


def _normalize_category(value: str | None, *, fallback_sink: dict[str, Any] | None = None) -> str:
    if value is None:
        return "note"
    normalized = re.sub(r"[\s-]+", "_", value.strip().casefold())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return "note"
    normalized = CATEGORY_ALIASES.get(normalized, normalized)
    _validate_category(normalized)
    if normalized not in CANONICAL_EVENT_CATEGORIES:
        # Soft fallback: agents keep inventing reasonable lane names, and a hard
        # ValueError used to bounce the whole write. Store under "note" and let
        # the caller surface what happened via `category_fallback`. Only input
        # that fails VALID_CATEGORY_RE (above) still raises.
        if fallback_sink is not None:
            fallback_sink["category_fallback"] = {"requested": normalized, "stored": "note"}
        return "note"
    return normalized


def _validate_relation(value: str) -> None:
    if not VALID_RELATION_RE.fullmatch(value):
        raise ValueError(f"Invalid relation type: {value}")


def _project_entity_id(project: str) -> str:
    slug = project.strip().casefold().replace(" ", "-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return f"project:{slug}"


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hours_since(value: str | None) -> float | None:
    parsed = _parse_iso_utc(value)
    if parsed is None:
        return None
    return max((datetime.now(timezone.utc) - parsed).total_seconds() / 3600.0, 0.0)


def _freshness_thresholds(
    manifest: dict[str, Any] | None = None,
    *,
    scope: str | None = None,
    item_id: str | None = None,
) -> dict[str, int]:
    thresholds = dict(FRESHNESS_THRESHOLDS_HOURS)
    if manifest is None or scope is None or item_id is None:
        return thresholds
    freshness = manifest.get("freshness") or {}
    scope_map = freshness.get(scope)
    if not isinstance(scope_map, dict):
        return thresholds
    policy = scope_map.get(item_id)
    if not isinstance(policy, dict):
        return thresholds
    for key in ("live", "recent", "stale"):
        override = policy.get(f"{key}_hours")
        if override is None:
            continue
        thresholds[key] = int(override)
    thresholds["recent"] = max(thresholds["recent"], thresholds["live"])
    thresholds["stale"] = max(thresholds["stale"], thresholds["recent"])
    return thresholds


def _freshness_status(value: str | None, *, thresholds: dict[str, int] | None = None) -> str:
    hours = _hours_since(value)
    resolved = thresholds or FRESHNESS_THRESHOLDS_HOURS
    if hours is None:
        return "unknown"
    if hours <= resolved["live"]:
        return "live"
    if hours <= resolved["recent"]:
        return "recent"
    if hours <= resolved["stale"]:
        return "stale"
    return "archival"


def _freshness_rank(value: str | None, *, thresholds: dict[str, int] | None = None) -> int:
    return FRESHNESS_RANK[_freshness_status(value, thresholds=thresholds)]


def _derived_source_trust_tier(source: dict[str, Any] | None) -> str:
    if source is None:
        return "unknown"
    role = source.get("role")
    if role in {"ssot", "strategy", "ownership", "global_context", "pipeline_metrics"}:
        return "operator_curated"
    if role in {"agent_protocol", "timeline_protocol", "architecture", "schema", "implementation_plan", "ops"}:
        return "reference"
    return "unknown"


def _resolve_source_trust_tier(source: dict[str, Any] | None) -> tuple[str, str]:
    if source is None:
        return "unknown", "derived"
    explicit = source.get("trust_tier")
    if explicit is None:
        return _derived_source_trust_tier(source), "derived"
    normalized = str(explicit).strip().casefold()
    if normalized in VALID_TRUST_TIERS:
        return normalized, "explicit"
    return _derived_source_trust_tier(source), "invalid"


# Identifiers, paths, and machine values: never free text, so never redacted.
_UNREDACTED_ENTRY_KEYS = frozenset(
    {
        "id", "request_id", "session_id", "agent", "domain", "category", "project",
        "task_id", "recorded_at", "occurred_at", "entity_id", "entity_type",
        "source_files", "slot", "kind", "supersedes", "mem0_status", "memory_guard",
        "content_hash", "skip_generic_source_archives", "agent_source",
    }
)


def _redact_entry(entry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    """Redact likely secrets in every free-text field of an entry, nested ones too."""
    redacted, counts = redact_value(entry, skip_keys=_UNREDACTED_ENTRY_KEYS)
    return redacted, dict(counts)


def _normalize_record_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(entry)
    normalized["domain"] = str(normalized.get("domain") or "global").strip() or "global"
    normalized["category"] = _normalize_category(normalized.get("category"), fallback_sink=normalized)
    normalized["text"] = str(normalized.get("text") or "").strip()
    if not normalized["text"]:
        raise ValueError("Event text is required.")
    why = normalized.get("why")
    normalized["why"] = str(why).strip() if why is not None and str(why).strip() else None
    source_files = normalized.get("source_files") or []
    normalized["source_files"] = [str(Path(item).expanduser()) for item in source_files if item]
    return normalized


def _memory_guard_visibility(entry: dict[str, Any]) -> str:
    memory_guard = entry.get("memory_guard")
    if isinstance(memory_guard, dict):
        return str(memory_guard.get("visibility") or "default")
    return "default"


def _filter_events_by_visibility(events: list[dict[str, Any]], *, visibility: str = "default") -> list[dict[str, Any]]:
    if visibility == "raw":
        return events
    return [entry for entry in events if _memory_guard_visibility(entry) != "raw_only"]


def _find_recent_exact_duplicate(
    config,
    entry: dict[str, Any],
    *,
    window_hours: int,
    limit: int = 100,
    connection=None,
) -> dict[str, Any] | None:
    target_time = _parse_iso_utc(entry.get("recorded_at")) or datetime.now(timezone.utc)
    recent_events = fetch_recent_events(
        config,
        limit=limit,
        domain=entry["domain"],
        visibility="raw",
        connection=connection,
    )
    for existing in recent_events:
        existing_time = _parse_iso_utc(existing.get("recorded_at"))
        if existing_time is None:
            continue
        delta_hours = abs((target_time - existing_time).total_seconds()) / 3600.0
        if delta_hours > window_hours:
            continue
        if (
            # Older events keep the spelling they were written with (FR-14).
            config.identities.domain(existing.get("domain") or "global") == entry["domain"]
            and (existing.get("category") or "note") == entry["category"]
            and (config.identities.project(existing.get("project")) or "") == (entry.get("project") or "")
            # One task under any spelling of its id (FR-14).
            and fold(existing.get("task_id") or "") == fold(entry.get("task_id") or "")
            and existing.get("checkpoint") == entry.get("checkpoint")
            and existing.get("fact") == entry.get("fact")
            and (existing.get("text") or "").strip() == entry["text"]
            and (existing.get("why") or None) == entry.get("why")
        ):
            return existing
    return None


def _guard_tokens(entry: dict[str, Any]) -> set[str]:
    text = " ".join(part for part in [entry.get("text") or "", entry.get("why") or ""] if part).casefold()
    return {token for token in re.findall(r"[a-zа-я0-9]+", text) if len(token) >= 2}


def _has_markdown_prompt_chatter(text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    bullet_lines = sum(1 for line in lines if line.startswith(("- ", "* ")) or re.match(r"^\d+\.\s+", line))
    return text.lstrip().startswith("#") or bullet_lines >= 2


def evaluate_memory_guard(config, entry: dict[str, Any], *, source_kind: str) -> dict[str, Any]:
    if source_kind != "chronicle_mcp":
        return {
            "version": MEMORY_GUARD_VERSION,
            "verdict": "durable",
            "visibility": "default",
            "score": 0,
            "signals": [],
            "source": "auto_mcp_guard",
            "reason": "Guard scope is limited to chronicle_mcp in v1.",
            "evaluated_at_utc": utc_now(),
        }

    text = entry.get("text") or ""
    lowered_text = text.casefold()
    why = entry.get("why") or ""
    signals: list[dict[str, Any]] = []
    raw_risk = 0
    evidence_bonus = 0

    if entry.get("category") in MEMORY_GUARD_SCOPED_CATEGORIES and any(term in lowered_text for term in MEMORY_GUARD_PROTOCOL_TERMS):
        raw_risk += 2
        signals.append({"name": "meta_protocol_instruction", "kind": "risk", "weight": 2})

    if any(term in lowered_text for term in MEMORY_GUARD_NEGATIVE_TERMS) and any(term in lowered_text for term in MEMORY_GUARD_POSITIVE_TERMS):
        raw_risk += 2
        signals.append({"name": "negative_mirror_instruction", "kind": "risk", "weight": 2})

    if not entry.get("source_files") and any(term in lowered_text for term in MEMORY_GUARD_ABSOLUTE_TERMS):
        raw_risk += 2
        signals.append({"name": "unsupported_absolute_claim", "kind": "risk", "weight": 2})

    if _has_markdown_prompt_chatter(text):
        raw_risk += 1
        signals.append({"name": "markdown_prompt_chatter", "kind": "risk", "weight": 1})

    target_tokens = _guard_tokens(entry)
    if target_tokens:
        recent_events = fetch_recent_events(config, limit=25, domain=entry["domain"], visibility="raw")
        target_time = _parse_iso_utc(entry.get("recorded_at")) or datetime.now(timezone.utc)
        for existing in recent_events:
            existing_time = _parse_iso_utc(existing.get("recorded_at"))
            if existing_time is None:
                continue
            if abs((target_time - existing_time).total_seconds()) > 24 * 3600:
                continue
            existing_tokens = _guard_tokens(existing)
            if not existing_tokens:
                continue
            overlap = len(target_tokens & existing_tokens) / len(target_tokens | existing_tokens)
            if overlap >= 0.85:
                raw_risk += 1
                signals.append({"name": "recent_near_duplicate", "kind": "risk", "weight": 1, "event_id": existing["id"]})
                break

    if len(why) >= 20:
        evidence_bonus += 1
        signals.append({"name": "why_present", "kind": "evidence", "weight": -1})
    if entry.get("project"):
        evidence_bonus += 1
        signals.append({"name": "project_present", "kind": "evidence", "weight": -1})
    if entry.get("source_files"):
        evidence_bonus += 1
        signals.append({"name": "source_files_present", "kind": "evidence", "weight": -1})
    if entry.get("category") in MEMORY_GUARD_EVIDENCE_CATEGORIES:
        evidence_bonus += 1
        signals.append({"name": "high_signal_category", "kind": "evidence", "weight": -1})

    score = raw_risk - evidence_bonus
    verdict = "local_only" if raw_risk >= 2 and score >= 2 else "durable"
    if verdict == "local_only":
        reason = "Guarded as local_only due to low-evidence protocol-like memory noise."
        visibility = "raw_only"
    else:
        reason = "Guard accepted the event as durable."
        visibility = "default"
    return {
        "version": MEMORY_GUARD_VERSION,
        "verdict": verdict,
        "visibility": visibility,
        "score": score,
        "signals": signals,
        "source": "auto_mcp_guard",
        "reason": reason,
        "evaluated_at_utc": utc_now(),
    }


def _lane_contracts(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    contracts = {lane_id: dict(payload, id=lane_id) for lane_id, payload in LANE_DEFAULTS.items()}
    for lane in manifest.get("lanes", []):
        lane_id = str(lane.get("id") or "").strip()
        if not lane_id:
            continue
        base = contracts.get(lane_id, {"id": lane_id})
        merged = dict(base)
        merged.update(lane)
        merged["id"] = lane_id
        merged.setdefault("label", lane_id.replace("_", " ").title())
        merged.setdefault("sensitive", False)
        merged.setdefault("default_enabled", not merged["sensitive"])
        contracts[lane_id] = merged
    return contracts


def _resolve_lane(source: dict[str, Any] | None) -> str:
    if source is None:
        return "work"
    explicit = source.get("lane")
    if explicit:
        return str(explicit).strip()
    return ROLE_TO_LANE.get(str(source.get("role") or "").strip(), "work")


def _lane_enabled(manifest: dict[str, Any], lane_id: str, *, source: dict[str, Any] | None = None) -> bool:
    contracts = _lane_contracts(manifest)
    lane = contracts.get(lane_id, {"default_enabled": True, "sensitive": False})
    if source is not None and source.get("enabled") is not None:
        return bool(source.get("enabled"))
    return bool(lane.get("default_enabled", not lane.get("sensitive", False)))


def _source_questions(source: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    if source is None:
        return [], []
    can = source.get("questions_it_can_answer")
    cannot = source.get("questions_it_cannot_answer")
    if isinstance(can, list) and isinstance(cannot, list):
        return [str(item) for item in can], [str(item) for item in cannot]
    defaults = SOURCE_ROLE_QUESTIONS.get(str(source.get("role") or "").strip(), {})
    return list(defaults.get("can") or []), list(defaults.get("cannot") or [])


def _source_owner(source_class: str, source: dict[str, Any] | None = None) -> str:
    if source and source.get("owner"):
        return str(source["owner"]).strip()
    if source_class == "semantic_recall":
        return "mem0"
    if source_class == "runtime_evidence":
        return "chronicle"
    if source_class == "derived_projection":
        return "chronicle"
    if source and source.get("role") in {"ops", "pipeline_metrics"}:
        return "system"
    return "operator"


def _source_freshness_policy(
    manifest: dict[str, Any],
    *,
    source_class: str,
    source_id: str,
) -> dict[str, int]:
    scope = {
        "ssot_source": "attach_sources",
        "runtime_evidence": "runtime_evidence",
        "semantic_recall": "semantic_recall",
        "derived_projection": "derived_projection",
    }.get(source_class)
    return _freshness_thresholds(manifest, scope=scope, item_id=source_id) if scope else dict(FRESHNESS_THRESHOLDS_HOURS)


def _source_catalog_entry(
    manifest: dict[str, Any],
    *,
    source_id: str,
    source_class: str,
    label: str,
    path: str | None,
    trust_tier: str,
    freshness_policy: dict[str, int],
    source: dict[str, Any] | None = None,
    source_role: str | None = None,
    exists: bool | None = None,
) -> dict[str, Any]:
    lane_id = _resolve_lane(source)
    can_answer, cannot_answer = _source_questions(source)
    return {
        "source_id": source_id,
        "id": source_id,
        "label": label,
        "path": path,
        "lane": lane_id,
        "class": source_class,
        "source_class": source_class,
        "role": source_role or (source.get("role") if source else None),
        "trust_tier": trust_tier,
        "freshness_policy": freshness_policy,
        "owner": _source_owner(source_class, source),
        "enabled": _lane_enabled(manifest, lane_id, source=source),
        "questions_it_can_answer": can_answer,
        "questions_it_cannot_answer": cannot_answer,
        "priority": source.get("priority", 0) if source else 0,
        "exists": exists,
    }


def _company_intel_path(manifest: dict[str, Any]) -> Path:
    explicit = (manifest.get("paths") or {}).get("company_intel_json")
    if explicit:
        return expand_path(explicit)
    return expand_path(manifest["paths"]["status_root"]) / "company-intel.json"


def _optional_runtime_path(manifest: dict[str, Any], path_key: str, fallback_filename: str | None = None) -> Path | None:
    explicit = (manifest.get("paths") or {}).get(path_key)
    if explicit:
        return expand_path(explicit)
    return None


def _resolve_runtime_source_path(manifest: dict[str, Any], source: dict[str, Any]) -> Path | None:
    explicit_path = source.get("path")
    if explicit_path:
        return expand_path(str(explicit_path))
    path_key = str(source.get("path_key") or source.get("id") or "").strip()
    fallback_filename = source.get("fallback_filename")
    if not path_key:
        return None
    if path_key == "company_intel_json":
        return _company_intel_path(manifest)
    return _optional_runtime_path(
        manifest,
        path_key,
        fallback_filename=str(fallback_filename) if fallback_filename else None,
    )


def _runtime_source_specs(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    explicit = {
        str(source.get("id") or "").strip(): dict(source)
        for source in manifest.get("runtime_sources", [])
        if str(source.get("id") or "").strip()
    }
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    for source_id, defaults in RUNTIME_SOURCE_DEFAULTS.items():
        row = dict(defaults)
        row.update(explicit.get(source_id, {}))
        row.setdefault("id", source_id)
        row.setdefault("path_key", source_id)
        ordered.append(row)
        seen.add(source_id)

    for source_id, source in explicit.items():
        if source_id in seen:
            continue
        row = dict(source)
        row.setdefault("id", source_id)
        row.setdefault("path_key", source_id)
        row.setdefault("label", source_id.replace("_", " ").title())
        ordered.append(row)
    return ordered


def _runtime_source_catalog(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in _runtime_source_specs(manifest):
        path = _resolve_runtime_source_path(manifest, source)
        if path is None:
            continue
        row = _source_catalog_entry(
            manifest,
            source_id=str(source["id"]),
            source_class="runtime_evidence",
            label=str(source.get("label") or source["id"]),
            path=str(path),
            trust_tier=str(source.get("trust_tier") or "canonical"),
            freshness_policy=_source_freshness_policy(
                manifest,
                source_class="runtime_evidence",
                source_id=str(source.get("path_key") or source["id"]),
            ),
            source=source,
            source_role=source.get("role"),
            exists=path.exists(),
        )
        row["trust_tier_source"] = "explicit" if source.get("trust_tier") else "default"
        rows.append(row)
    return rows


def _domain_id(manifest: dict[str, Any], domain_id: str | None) -> str | None:
    """A domain's canonical id for any of its spellings (FR-14); an unknown one as given."""
    entry = manifest["domain_map"].get(domain_id) if domain_id else None
    return entry["id"] if entry else domain_id


def build_sources_audit(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
) -> dict[str, Any]:
    domain_id = _domain_id(manifest, domain_id)
    attach_catalog = _source_catalog(manifest, domain_id)
    runtime_catalog = _runtime_source_catalog(manifest)
    mem0_dump_path = _compat_path(manifest, "mem0_dump")
    mem0_entry = _source_catalog_entry(
        manifest,
        source_id="mem0_dump",
        source_class="semantic_recall",
        label="Mem0 Dump",
        path=str(mem0_dump_path),
        trust_tier="semantic_recall",
        freshness_policy=_source_freshness_policy(manifest, source_class="semantic_recall", source_id="mem0_dump"),
        source={"id": "mem0_dump", "lane": "decisions"},
        exists=mem0_dump_path.exists(),
    )
    latest_projection = _latest_projection_row(manifest)
    projection_entry = _source_catalog_entry(
        manifest,
        source_id="latest_projection",
        source_class="derived_projection",
        label=latest_projection.get("projection_name") or "Latest Projection",
        path=latest_projection.get("path"),
        trust_tier="reference",
        freshness_policy=_source_freshness_policy(
            manifest,
            source_class="derived_projection",
            source_id="latest_projection",
        ),
        source={"id": "latest_projection", "lane": "vectors"},
        exists=bool(latest_projection.get("exists")),
    )

    freshness = build_freshness_audit(manifest, domain_id=domain_id)
    lane_summary: dict[str, dict[str, Any]] = {}
    all_entries = [*attach_catalog, *runtime_catalog, mem0_entry, projection_entry]
    for entry in all_entries:
        lane_id = entry["lane"]
        lane_contract = _lane_contracts(manifest).get(lane_id, {"label": lane_id.replace("_", " ").title(), "sensitive": False})
        summary = lane_summary.setdefault(
            lane_id,
            {
                "lane": lane_id,
                "label": lane_contract["label"],
                "sensitive": bool(lane_contract.get("sensitive", False)),
                "enabled": bool(lane_contract.get("default_enabled", True)),
                "source_count": 0,
                "enabled_sources": 0,
                "stale_sources": 0,
                "sources": [],
            },
        )
        summary["enabled"] = summary["enabled"] or bool(entry.get("enabled"))
        summary["source_count"] += 1
        summary["enabled_sources"] += 1 if entry.get("enabled") else 0
        summary["sources"].append(entry["source_id"])

    freshness_rows = [
        *freshness.get("attach_sources", []),
        *freshness.get("runtime_evidence", []),
        freshness.get("mem0_dump", {}),
        freshness.get("latest_projection", {}),
    ]
    by_source_id = {entry["source_id"]: entry for entry in all_entries}
    for row in freshness_rows:
        source_id = row.get("id")
        entry = by_source_id.get(source_id)
        if not entry:
            continue
        entry["freshness_status"] = row.get("freshness_status")
        entry["mtime"] = row.get("mtime")
        lane_summary[entry["lane"]]["stale_sources"] += 1 if row.get("freshness_status") in {"stale", "archival"} else 0

    coverage = {
        "domain_id": domain_id,
        "total_sources": len(all_entries),
        "enabled_sources": sum(1 for entry in all_entries if entry.get("enabled")),
        "disabled_sensitive_lanes": sorted(
            lane_id
            for lane_id, payload in lane_summary.items()
            if payload.get("sensitive") and not payload.get("enabled")
        ),
        "lanes": sorted(lane_summary.values(), key=lambda item: item["lane"]),
    }

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "domain": {"id": domain_id, "label": manifest["domain_map"][domain_id]["label"]},
        "status": freshness["status"],
        "issue_count": freshness["issue_count"],
        "source_catalog_schema_version": "2026-03-21.v1",
        "lane_policy_schema_version": "2026-03-21.v1",
        "source_catalog": sorted(all_entries, key=lambda item: (-item.get("priority", 0), item["source_id"])),
        "freshness_audit": freshness,
        "coverage": coverage,
    }


def _source_catalog(manifest: dict[str, Any], domain_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_id in manifest["domain_map"][domain_id]["source_ids"]:
        source = manifest["source_map"][source_id]
        trust_tier, trust_tier_source = _resolve_source_trust_tier(source)
        row = _source_catalog_entry(
            manifest,
            source_id=source_id,
            source_class="ssot_source",
            label=source["label"],
            path=source["path"],
            trust_tier=trust_tier,
            freshness_policy=_source_freshness_policy(manifest, source_class="ssot_source", source_id=source_id),
            source=source,
            source_role=source.get("role"),
            exists=expand_path(source["path"]).exists(),
        )
        row["trust_tier_source"] = trust_tier_source
        rows.append(row)
    return rows


def _annotate_freshness_row(
    row: dict[str, Any],
    *,
    source_class: str,
    trust_tier: str,
    source_role: str | None = None,
    freshness_thresholds: dict[str, int] | None = None,
) -> dict[str, Any]:
    annotated = dict(row)
    thresholds = dict(freshness_thresholds or FRESHNESS_THRESHOLDS_HOURS)
    annotated["source_class"] = source_class
    annotated["trust_tier"] = trust_tier
    annotated["trust_rank"] = TRUST_TIER_RANK.get(trust_tier, 0)
    if source_role is not None:
        annotated["source_role"] = source_role
    annotated["freshness_thresholds_hours"] = thresholds
    annotated["freshness_status"] = _freshness_status(annotated.get("mtime"), thresholds=thresholds)
    annotated["freshness_rank"] = _freshness_rank(annotated.get("mtime"), thresholds=thresholds)
    return annotated


def _runtime_evidence_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in _runtime_source_specs(manifest):
        source_id = str(source["id"])
        path_key = str(source.get("path_key") or source_id)
        path = _resolve_runtime_source_path(manifest, source)
        if path is None:
            continue
        row: dict[str, Any] = {
            "id": source_id,
            "label": str(source.get("label") or source_id),
            "path": str(path),
            "path_key": path_key,
            "exists": path.exists(),
        }
        if path.exists():
            row.update(file_meta(path))
        rows.append(
            _annotate_freshness_row(
                row,
                source_class="runtime_evidence",
                trust_tier=str(source.get("trust_tier") or "canonical"),
                source_role=source.get("role"),
                freshness_thresholds=_freshness_thresholds(
                    manifest,
                    scope="runtime_evidence",
                    item_id=path_key,
                ),
            )
        )
    return rows


def _latest_projection_row(manifest: dict[str, Any]) -> dict[str, Any]:
    latest_projection = fetch_latest_projection_run(_config(manifest))
    thresholds = _freshness_thresholds(
        manifest,
        scope="derived_projection",
        item_id="latest_projection",
    )
    if latest_projection is None:
        return _annotate_freshness_row(
            {
                "id": "latest_projection",
                "label": "Latest Projection",
                "path": None,
                "exists": False,
                "rendered_at_utc": None,
                "projection_name": None,
                "status": None,
                "snapshot_id": None,
                "target_exists": None,
            },
            source_class="derived_projection",
            trust_tier="reference",
            freshness_thresholds=thresholds,
        )

    target_path = latest_projection.get("target_path")
    target_exists = Path(target_path).exists() if target_path else False
    return _annotate_freshness_row(
        {
            "id": "latest_projection",
            "label": "Latest Projection",
            "path": target_path,
            "exists": True,
            "mtime": latest_projection.get("rendered_at_utc"),
            "projection_name": latest_projection.get("projection_name"),
            "status": latest_projection.get("status"),
            "snapshot_id": latest_projection.get("snapshot_id"),
            "rendered_at_utc": latest_projection.get("rendered_at_utc"),
            "target_exists": target_exists,
        },
        source_class="derived_projection",
        trust_tier="reference",
        freshness_thresholds=thresholds,
    )


def _freshness_issue(
    severity: str,
    kind: str,
    detail: str,
    *,
    source_id: str | None = None,
    label: str | None = None,
    path: str | None = None,
    freshness_status: str | None = None,
    observed_at_utc: str | None = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "kind": kind,
        "detail": detail,
        "source_id": source_id,
        "label": label,
        "path": path,
        "freshness_status": freshness_status,
        "observed_at_utc": observed_at_utc,
    }


def build_freshness_audit(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
) -> dict[str, Any]:
    domain_id = _domain_id(manifest, domain_id)
    domain = manifest["domain_map"][domain_id]
    config = _config(manifest)
    attach_sources: list[dict[str, Any]] = []
    for row in source_freshness(manifest, domain["source_ids"]):
        source = manifest["source_map"][row["id"]]
        trust_tier, trust_tier_source = _resolve_source_trust_tier(source)
        annotated = _annotate_freshness_row(
            row,
            source_class="ssot_source",
            trust_tier=trust_tier,
            source_role=source.get("role"),
            freshness_thresholds=_freshness_thresholds(
                manifest,
                scope="attach_sources",
                item_id=row["id"],
            ),
        )
        annotated["source_priority"] = source.get("priority", 0)
        annotated["trust_tier_source"] = trust_tier_source
        attach_sources.append(annotated)

    runtime_evidence = _runtime_evidence_rows(manifest)
    mem0_dump = _annotate_freshness_row(
        mem0_meta(manifest),
        source_class="semantic_recall",
        trust_tier="semantic_recall",
        freshness_thresholds=_freshness_thresholds(
            manifest,
            scope="semantic_recall",
            item_id="mem0_dump",
        ),
    )
    latest_projection = _latest_projection_row(manifest)
    latest_snapshot = fetch_latest_snapshot(config, domain=domain_id) or fetch_latest_snapshot(config)

    issues: list[dict[str, Any]] = []
    for row in attach_sources:
        if row.get("trust_tier_source") == "invalid":
            issues.append(
                _freshness_issue(
                    "critical",
                    "source_contract_invalid_trust_tier",
                    f"Attach source `{row['label']}` has an invalid explicit trust tier.",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                )
            )
        elif row.get("trust_tier_source") != "explicit":
            issues.append(
                _freshness_issue(
                    "warn",
                    "source_contract_missing_trust_tier",
                    f"Attach source `{row['label']}` relies on derived trust tier `{row['trust_tier']}` instead of an explicit manifest contract.",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                )
            )
        if not row.get("exists"):
            severity = "critical" if row.get("trust_tier") in {"canonical", "operator_curated"} else "warn"
            issues.append(
                _freshness_issue(
                    severity,
                    "attach_source_missing",
                    f"Attach source `{row['label']}` is missing.",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                )
            )
            continue
        if row.get("freshness_status") in {"stale", "archival"}:
            issues.append(
                _freshness_issue(
                    "warn",
                    "attach_source_stale",
                    f"Attach source `{row['label']}` is {row['freshness_status']} (last updated {row.get('mtime')}).",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                    freshness_status=row.get("freshness_status"),
                    observed_at_utc=row.get("mtime"),
                )
            )

    for row in runtime_evidence:
        if not row.get("exists"):
            issues.append(
                _freshness_issue(
                    "warn",
                    "runtime_evidence_missing",
                    f"Runtime evidence `{row['label']}` is missing.",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                )
            )
            continue
        if row.get("freshness_status") in {"stale", "archival"}:
            issues.append(
                _freshness_issue(
                    "warn",
                    "runtime_evidence_stale",
                    f"Runtime evidence `{row['label']}` is {row['freshness_status']} (last updated {row.get('mtime')}).",
                    source_id=row.get("id"),
                    label=row.get("label"),
                    path=row.get("path"),
                    freshness_status=row.get("freshness_status"),
                    observed_at_utc=row.get("mtime"),
                )
            )

    if not mem0_dump.get("exists"):
        issues.append(
            _freshness_issue(
                "warn",
                "mem0_dump_missing",
                "Mem0 dump is missing.",
                source_id="mem0_dump",
                label="Mem0 Dump",
                path=mem0_dump.get("path"),
            )
        )
    elif mem0_dump.get("freshness_status") in {"stale", "archival"}:
        issues.append(
            _freshness_issue(
                "warn",
                "mem0_dump_stale",
                f"Mem0 dump is {mem0_dump['freshness_status']} (last updated {mem0_dump.get('mtime')}).",
                source_id="mem0_dump",
                label="Mem0 Dump",
                path=mem0_dump.get("path"),
                freshness_status=mem0_dump.get("freshness_status"),
                observed_at_utc=mem0_dump.get("mtime"),
            )
        )

    if latest_projection.get("exists"):
        if latest_projection.get("target_exists") is False:
            issues.append(
                _freshness_issue(
                    "warn",
                    "projection_target_missing",
                    "Latest projection run points to a missing target file.",
                    source_id="latest_projection",
                    label=latest_projection.get("projection_name") or latest_projection.get("label"),
                    path=latest_projection.get("path"),
                )
            )
        if latest_projection.get("freshness_status") in {"stale", "archival"}:
            issues.append(
                _freshness_issue(
                    "warn",
                    "projection_stale",
                    f"Latest projection is {latest_projection['freshness_status']} (rendered {latest_projection.get('rendered_at_utc')}).",
                    source_id="latest_projection",
                    label=latest_projection.get("projection_name") or latest_projection.get("label"),
                    path=latest_projection.get("path"),
                    freshness_status=latest_projection.get("freshness_status"),
                    observed_at_utc=latest_projection.get("rendered_at_utc"),
                )
            )
    elif latest_snapshot is not None:
        issues.append(
            _freshness_issue(
                "warn",
                "projection_missing",
                "Chronicle has a snapshot but no recorded projection run.",
                source_id="latest_projection",
                label="Latest Projection",
                path=latest_projection.get("path"),
            )
        )

    issues.sort(
        key=lambda item: (
            -ISSUE_SEVERITY_RANK.get(item["severity"], 0),
            item["kind"],
            item.get("label") or "",
        )
    )
    status = "ok"
    if any(item["severity"] == "critical" for item in issues):
        status = "critical"
    elif issues:
        status = "warn"

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "domain": {
            "id": domain_id,
            "label": domain["label"],
        },
        "status": status,
        "issue_count": len(issues),
        "warnings": [item["detail"] for item in issues],
        "issues": issues,
        "attach_sources": attach_sources,
        "runtime_evidence": runtime_evidence,
        "mem0_dump": mem0_dump,
        "latest_projection": latest_projection,
    }


def _resolve_activation_snapshot(
    manifest: dict[str, Any],
    *,
    domain_id: str,
    agent: str,
    title: str | None,
    focus: str | None,
    capture: bool,
) -> dict[str, Any]:
    if capture:
        return capture_runtime_snapshot(
            manifest,
            domain_id=domain_id,
            agent=agent,
            title=title,
            focus=focus,
        )

    config = _config(manifest)
    latest = fetch_latest_snapshot(config, domain=domain_id) or fetch_latest_snapshot(config)
    if latest is None:
        recent_events = fetch_recent_events(config, limit=8, domain=domain_id)
        if not recent_events:
            recent_events = _filter_events_by_visibility(sorted(
                read_jsonl(_compat_path(manifest, "ledger_file")),
                key=lambda row: row.get("recorded_at", ""),
                reverse=True,
            )[:8])
        return build_runtime_snapshot(
            manifest,
            domain_id=domain_id,
            agent=agent,
            title=title,
            focus=focus,
            recent_events=recent_events,
        )
    return latest


def build_attach_bundle(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    agent: str = "codex",
    title: str | None = None,
    focus: str | None = None,
    capture: bool = True,
    event_limit: int = 8,
) -> dict[str, Any]:
    snapshot = _resolve_activation_snapshot(
        manifest,
        domain_id=domain_id,
        agent=agent,
        title=title,
        focus=focus,
        capture=capture,
    )
    config = _config(manifest)
    recent_events = fetch_recent_events(config, limit=event_limit, domain=domain_id)
    if not recent_events:
        recent_events = _filter_events_by_visibility(snapshot.get("recent_ledger", [])[:event_limit])
    with open_connection(config) as connection:
        summary = database_summary(connection)

    domain = manifest["domain_map"][domain_id]
    freshness_audit = build_freshness_audit(manifest, domain_id=domain_id)
    source_audit = build_sources_audit(manifest, domain_id=domain_id)
    normalized_entities = fetch_normalized_entities(config, limit=200)
    if not normalized_entities:
        normalized_entities = build_normalized_entities(
            manifest,
            domain_id=domain_id,
            snapshot=snapshot,
            event_limit=max(event_limit, 24),
        )
    attach_sources = _source_catalog(manifest, domain_id)
    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "attach_bundle_schema_version": V2_ATTACH_BUNDLE_VERSION,
        "source_catalog_schema_version": source_audit["source_catalog_schema_version"],
        "lane_policy_schema_version": source_audit["lane_policy_schema_version"],
        "domain": {
            "id": domain_id,
            "label": domain["label"],
        },
        "snapshot": snapshot,
        "db_summary": summary,
        "current_truth_summary": {
            "domain": domain_id,
            "snapshot_id": snapshot.get("id"),
            "latest_snapshot_at": snapshot.get("captured_at_utc"),
            "recent_event_ids": [item["id"] for item in recent_events],
            "recent_event_count": len(recent_events),
        },
        "recent_events": recent_events,
        "freshness_audit": freshness_audit,
        "source_catalog": attach_sources,
        "source_audit": source_audit,
        "normalized_entities": normalized_entities[:40],
        "query_modes": list(QUERY_MODES),
        "truth_order": [
            "chronicle.db",
            "chronicle event stream",
            "runtime evidence",
            "status markdown",
            "mem0 semantic recall",
        ],
    }


STARTUP_BUNDLE_SCHEMA_VERSION = "2026-03-29.v1"


def _build_startup_runtime_view(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Extract compact runtime view from a snapshot for startup bundle."""
    portfolio_assets = snapshot.get("portfolio_assets", {})
    return {
        "snapshot_meta": {
            "id": snapshot.get("id"),
            "captured_at_utc": snapshot.get("captured_at_utc"),
            "captured_at_local": snapshot.get("captured_at_local"),
            "timezone": snapshot.get("timezone"),
            "title": snapshot.get("title"),
            "focus": snapshot.get("focus"),
        },
        "portfolio": {
            "projects_total": portfolio_assets.get("projects_total", 0),
            "missing_assets_total": portfolio_assets.get("missing_assets_total", 0),
            "og_images_needed": portfolio_assets.get("og_images_needed", 0),
            "missing_assets_sample": portfolio_assets.get("missing_assets_sample", [])[:5],
        },
        "repos": [
            {
                "path": repo.get("path"),
                "branch": repo.get("branch"),
                "dirty": repo.get("dirty", False),
                "change_count": repo.get("change_count", 0),
            }
            for repo in snapshot.get("repos", [])
        ],
    }


def _build_startup_source_health(
    freshness_audit: dict[str, Any],
    *,
    compact: bool = False,
) -> dict[str, Any]:
    """Extract compact source health from freshness audit for startup bundle."""
    issues_limit = 6 if compact else 12
    return {
        "status": freshness_audit.get("status"),
        "issue_count": freshness_audit.get("issue_count", 0),
        "issues": [
            {
                "severity": issue.get("severity"),
                "kind": issue.get("kind"),
                "detail": issue.get("detail"),
                "source_id": issue.get("source_id"),
                "freshness_status": issue.get("freshness_status"),
            }
            for issue in freshness_audit.get("issues", [])[:issues_limit]
        ],
    }


def _build_startup_sources(
    manifest: dict[str, Any],
    domain_id: str,
    *,
    compact: bool = False,
) -> list[dict[str, Any]]:
    """Build enriched source content list for startup bundle."""
    domain_config = manifest["domain_map"][domain_id]
    source_ids = domain_config.get("source_ids", [])
    results: list[dict[str, Any]] = []
    for source_id in source_ids:
        source_config = manifest["source_map"][source_id]
        source_data = read_source_content(source_config)
        raw_content = source_data.get("content", "")
        content_truncated = False
        if compact and len(raw_content) > 1200:
            raw_content = shorten(raw_content, 1200)
            content_truncated = True
        entry: dict[str, Any] = {
            "id": source_id,
            "label": source_config.get("label", ""),
            "path": source_data.get("path", source_config.get("path", "")),
            "role": source_config.get("role"),
            "trust_tier": source_config.get("trust_tier", "reference"),
            "exists": source_data.get("exists", False),
            "mtime": source_data.get("mtime"),
            "size": source_data.get("size", 0),
            "content": raw_content,
        }
        if compact:
            entry["content_truncated"] = content_truncated
        results.append(entry)
    return results


def _build_startup_entity_digest(
    normalized_entities: list[dict[str, Any]],
    *,
    compact: bool = False,
) -> list[dict[str, Any]]:
    """Build compact entity digest for startup bundle.

    Handles both persisted format (id, source_refs) and cold-start format
    (entity_id, source_ref_count, metadata).
    """
    entity_limit = 10 if compact else 20
    result: list[dict[str, Any]] = []
    for entity in normalized_entities[:entity_limit]:
        entity_id = entity.get("id") or entity.get("entity_id")
        source_refs = entity.get("source_refs")
        if isinstance(source_refs, list):
            ref_count = len(source_refs)
        else:
            ref_count = entity.get("source_ref_count", 0)
        metadata = entity.get("metadata") or {}
        lane = entity.get("lane") or metadata.get("lane")
        result.append({
            "id": entity_id,
            "entity_type": entity.get("entity_type"),
            "canonical_name": entity.get("canonical_name"),
            "aliases": entity.get("aliases", []),
            "source_ref_count": ref_count,
            "lane": lane,
        })
    return result


# Mem0 sync bookkeeping: plumbing, not memory, so a startup bundle leaves it out.
_SYNC_FIELDS = frozenset({"mem0_status", "mem0_error", "mem0_raw", "mem0_synced_at"})


def _startup_event(event: dict[str, Any], *, shown_checkpoint: str | None) -> dict[str, Any]:
    """An event as a startup bundle shows it: no sync bookkeeping, and no second
    copy of the checkpoint that task_context already carries."""
    trimmed = {key: value for key, value in event.items() if key not in _SYNC_FIELDS}
    if shown_checkpoint is not None and trimmed.get("id") == shown_checkpoint:
        trimmed.pop("checkpoint", None)
    return trimmed


def build_startup_bundle(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    agent: str = "codex",
    title: str | None = None,
    focus: str | None = None,
    capture: bool = False,
    limit: int = 3,
    compact: bool = False,
    project: str | None = None,
    task_id: str | None = None,
    since: str | None = None,
    before: str | None = None,
) -> dict[str, Any]:
    """Build a startup brief bundle for an agent, assembled directly without nesting attach_bundle.

    This is an independent view optimized for startup context delivery.
    It does NOT call build_attach_bundle — all data is fetched directly.

    Args:
        manifest: Loaded SSOT manifest.
        domain_id: Domain to build bundle for.
        agent: Agent identifier.
        title: Optional snapshot title.
        focus: Optional focus string.
        capture: Whether to capture a new snapshot.
        limit: Number of recent events to include.
        compact: If True, truncate sources and reduce entity/issue counts.

    Returns:
        Startup bundle dict targeting ~50K full / ~30K compact.
    """
    from .memory import task_context as read_task_context
    identities = _config(manifest).identities
    domain_id = identities.domain(domain_id) or "global"
    project = identities.project(project)
    if domain_id not in manifest["domain_map"]:
        raise ValueError(f"Unknown domain {domain_id!r}; use a manifest domain and project/task_id for task scope")
    context = read_task_context(manifest, domain=domain_id, project=project,
                                task_id=task_id, since=since, before=before, limit=limit)
    # 1. Resolve snapshot (reuse or capture)
    snapshot = _resolve_activation_snapshot(
        manifest,
        domain_id=domain_id,
        agent=agent,
        title=title,
        focus=focus,
        capture=capture,
    )

    # 2. Fetch recent events directly
    config = _config(manifest)
    recent_events = fetch_recent_events(config, limit=limit, domain=domain_id)
    recall_status = None
    if focus:
        # The bundle shows events only, so its confidence must come from events too.
        recall = query_memory(manifest, query=focus, domain=domain_id, project=project,
                              task_id=task_id, limit=limit, include_notes=False)
        recall_status = {key: recall[key] for key in (
            "degraded", "channel_errors", "vector_coverage", "relaxed", "no_confident_match", "hint",
        ) if key in recall}
        recent_events = [event for hit in recall["results"]
                         if (event := fetch_event(config, event_id=hit["event_id"])) is not None]
        if not (project or task_id):
            matching = {event["id"] for event in recent_events}
            context["changes"] = [change for change in context["changes"] if change["event_id"] in matching]
            context["current_facts"] = [fact for fact in context["current_facts"] if fact["event_id"] in matching]
            if context["checkpoint"] and context["checkpoint"]["event_id"] not in matching:
                context["checkpoint"] = None
    elif project or task_id:
        recent_events = []
        seen = set()
        for change in reversed(context["changes"]):
            if change["event_id"] not in seen:
                event = fetch_event(config, event_id=change["event_id"])
                if event:
                    recent_events.append(event)
                    seen.add(change["event_id"])
    if not recent_events and not (focus or project or task_id):
        recent_events = _filter_events_by_visibility(
            snapshot.get("recent_ledger", [])[:limit]
        )
    shown_checkpoint = (context.get("checkpoint") or {}).get("event_id")
    recent_events = [_startup_event(event, shown_checkpoint=shown_checkpoint) for event in recent_events]

    # 3. Build enriched sources (full content or truncated)
    sources = _build_startup_sources(manifest, domain_id, compact=compact)
    if compact and (focus or project or task_id):
        sources = [{key: value for key, value in source.items()
                    if key not in {"content", "content_truncated"}} for source in sources]

    # 4. Freshness audit → compact source health
    freshness_audit = build_freshness_audit(manifest, domain_id=domain_id)
    source_health = _build_startup_source_health(freshness_audit, compact=compact)

    # 5. Runtime view (compact extract from snapshot)
    runtime = _build_startup_runtime_view(snapshot)
    if compact and (project or task_id or focus):
        runtime["portfolio"] = {}
        runtime["repos"] = [repo for repo in runtime["repos"]
                            if project and Path(repo.get("path") or "").name == project]

    # 6. Normalized entities → compact digest
    normalized_entities = fetch_normalized_entities(config, limit=200)
    if not normalized_entities:
        normalized_entities = build_normalized_entities(
            manifest,
            domain_id=domain_id,
            snapshot=snapshot,
            event_limit=max(limit, 24),
        )
    entity_digest = _build_startup_entity_digest(
        normalized_entities, compact=compact
    )
    if compact and (project or task_id or focus):
        entity_digest = [entity for entity in entity_digest
                         if project and project.casefold() in {
                             str(name).casefold() for name in
                             [entity.get("canonical_name"), *entity.get("aliases", [])]
                         }]

    # 8. Mem0 hits (from snapshot, no duplication)
    mem0_dump_hits = {} if (focus or project or task_id) else snapshot.get("mem0_snapshot_hits", {})

    # 9. DB summary
    with open_connection(config) as connection:
        db_summary = database_summary(connection)

    domain_config = manifest["domain_map"][domain_id]
    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "startup_bundle_schema_version": STARTUP_BUNDLE_SCHEMA_VERSION,
        "generated_at": utc_now(),
        "snapshot_at": snapshot.get("captured_at_utc"),
        "focus": focus,
        "task_context": context,
        "recall_status": recall_status,
        "domain": {
            "id": domain_id,
            "label": domain_config["label"],
        },
        "compact": compact,
        "runtime": runtime,
        "source_health": source_health,
        "sources": sources,
        "recent_events": recent_events,
        "mem0_dump_hits": mem0_dump_hits,
        "entity_digest": entity_digest,
        "db_summary": db_summary,
        "query_modes": list(QUERY_MODES),
        "truth_order": [
            "chronicle.db",
            "chronicle event stream",
            "runtime evidence",
            "status markdown",
            "mem0 semantic recall",
        ],
    }


def _clean_markdown_text(value: str) -> str:
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    cleaned = re.sub(r"`([^`]*)`", r"\1", cleaned)
    cleaned = re.sub(r"[*_~>#]+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" .,-")


def _parse_labeled_bullet(line: str) -> tuple[str | None, str | None]:
    match = re.match(r"^-\s+(?:\*\*)?([^:*]+?)(?:\*\*)?:\s*(.+)$", line.strip())
    if not match:
        return None, None
    label = normalize_heading(match.group(1))
    value = _clean_markdown_text(match.group(2))
    return label, value or None


def _normalize_company_candidate(value: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", value.strip()).strip(".,;:!?")
    if not cleaned:
        return None
    cleaned = cleaned.replace("—", "-").replace("–", "-")
    if "..." in cleaned:
        cleaned = cleaned.split("...", 1)[0].strip(" .,-")
    if " - " in cleaned:
        parts = [part.strip(" .,-") for part in cleaned.split(" - ") if part.strip(" .,-")]
        if len(parts) >= 2:
            left = parts[0]
            right = parts[-1]
            right_fold = right.casefold()
            left_fold = left.casefold()
            if right_fold in COMPANY_SOURCE_SUFFIXES or right_fold.endswith(".com") or right_fold.endswith("jobs"):
                cleaned = left
            elif left_fold in COMPANY_SOURCE_PREFIXES:
                cleaned = right
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")
    cleaned = cleaned.rstrip("&/ ").strip()
    lowered = cleaned.casefold().replace("&", " and ")
    if not lowered or lowered in GENERIC_COMPANY_ALIASES:
        return None
    if len(lowered) <= 2 and lowered != "ai":
        return None
    if any(fragment in lowered for fragment in (" ai-driven ", "rapid iteration", " key insights", " key takeaways")):
        return None
    if len(lowered.split()) > 6:
        return None
    return cleaned or None


def _normalize_entity_alias(entity_type: str, alias: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", alias.strip()).strip()
    if not cleaned:
        return None
    if entity_type == "company":
        return _normalize_company_candidate(cleaned)
    return cleaned


def _has_noisy_alias(entity: dict[str, Any]) -> bool:
    if entity.get("entity_type") != "company":
        return False
    for alias in entity.get("aliases", []):
        normalized = _normalize_company_candidate(alias)
        if normalized is None:
            return True
        if _clean_markdown_text(alias) != normalized:
            return True
    return False


def _entity_name_preference(entity_type: str, value: str) -> tuple[int, int]:
    lowered = value.casefold()
    tokens = [token for token in re.findall(r"[a-zа-я0-9]+", lowered) if token]
    score = 0
    if entity_type == "company":
        if any(token in COMPANY_SUFFIX_TOKENS for token in tokens):
            score -= 2
        if any(token in GENERIC_COMPANY_ALIASES for token in (lowered, *tokens)):
            score -= 5
    score -= max(len(tokens) - 2, 0)
    return score, -len(value)


def _entity_display_name(entity_type: str, alias: str) -> str:
    cleaned = re.sub(r"\s+", " ", alias.strip().strip(".,;:")).strip()
    if entity_type == "company" and cleaned.islower():
        return cleaned.title()
    if entity_type in {"project", "system", "topic"}:
        return cleaned.replace("-", " ").strip().title()
    return cleaned


def canonicalize_entity_name(entity_type: str, value: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", value.strip()).strip(".,;:!?")
    lowered = cleaned.casefold()
    lowered = lowered.replace("&", " and ")
    tokens = [token for token in re.findall(r"[a-zа-я0-9]+", lowered) if token]
    if entity_type == "company":
        while len(tokens) > 1 and tokens[-1] in COMPANY_SUFFIX_TOKENS:
            tokens.pop()
    canonical_key = "-".join(tokens) or re.sub(r"[^a-z0-9]+", "-", lowered).strip("-") or "unknown"
    return canonical_key, _entity_display_name(entity_type, cleaned)


def _source_ref(source_type: str, source_id: str, value: str, *, path: str | None = None) -> dict[str, Any]:
    payload = {
        "source_type": source_type,
        "source_id": source_id,
        "value": value,
    }
    if path:
        payload["path"] = path
    return payload


def build_normalized_entities(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    snapshot: dict[str, Any] | None = None,
    event_limit: int = 50,
) -> list[dict[str, Any]]:
    config = _config(manifest)
    resolved_snapshot = snapshot or fetch_latest_snapshot(config, domain=domain_id) or fetch_latest_snapshot(config)
    recent_events = fetch_recent_events(config, limit=event_limit, domain=domain_id)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    work_enabled = _lane_enabled(manifest, "work")
    agents_enabled = _lane_enabled(manifest, "agents")
    world_enabled = _lane_enabled(manifest, "world")
    companies_enabled = _lane_enabled(manifest, "companies")
    decisions_enabled = _lane_enabled(manifest, "decisions")

    def add(entity_type: str, alias: str, source_ref: dict[str, Any], *, metadata: dict[str, Any] | None = None) -> None:
        normalized_alias = _normalize_entity_alias(entity_type, alias)
        if not normalized_alias:
            return
        canonical_key, canonical_name = canonicalize_entity_name(entity_type, normalized_alias)
        key = (entity_type, canonical_key)
        group = groups.setdefault(
            key,
            {
                "entity_type": entity_type,
                "canonical_key": canonical_key,
                "canonical_name": canonical_name,
                "aliases": set(),
                "source_refs": [],
                "status": "active",
                "metadata": dict(metadata or {}),
            },
        )
        if _entity_name_preference(entity_type, canonical_name) > _entity_name_preference(entity_type, group["canonical_name"]):
            group["canonical_name"] = canonical_name
        group["aliases"].add(_entity_display_name(entity_type, normalized_alias))
        if source_ref not in group["source_refs"]:
            group["source_refs"].append(source_ref)
        if metadata:
            group["metadata"].update(metadata)

    for event in recent_events:
        if work_enabled and event.get("project"):
            add(
                "project",
                event["project"],
                _source_ref("event", event["id"], event["project"]),
                metadata={"lane": "work"},
            )

    for system_name in ("chronicle", "mem0"):
        if system_name == "mem0" and not agents_enabled:
            continue
        if system_name == "chronicle" and not decisions_enabled:
            continue
        add(
            "system",
            system_name,
            _source_ref("system", system_name, system_name),
            metadata={
                "lane": (
                    "agents"
                    if system_name == "mem0"
                    else "decisions"
                )
            },
        )

    if resolved_snapshot:
        portfolio = resolved_snapshot.get("portfolio_assets") or {}
        if work_enabled and portfolio.get("exists"):
            add(
                "project",
                "portfolio",
                _source_ref("snapshot", resolved_snapshot["id"], "portfolio"),
                metadata={"lane": "work"},
            )

        digest = resolved_snapshot.get("digest") or {}
        if world_enabled:
            for headline in digest.get("world_headlines", [])[:10]:
                bracket_matches = re.findall(r"\[([^\]]+)\]", headline)
                for bracket in bracket_matches:
                    for raw_topic in re.split(r"\s*/\s*|\s*\+\s*", bracket):
                        topic = raw_topic.strip()
                        if not topic:
                            continue
                        add(
                            "topic",
                            topic,
                            _source_ref("snapshot", resolved_snapshot["id"], topic, path=digest.get("synthesis_path")),
                            metadata={"lane": "world"},
                        )

    company_intel_path = _company_intel_path(manifest)
    company_intel = read_json(company_intel_path) or {}
    if companies_enabled and isinstance(company_intel, dict):
        for _, payload in (company_intel.get("companies") or {}).items():
            name = payload.get("name") if isinstance(payload, dict) else None
            if name:
                add(
                    "company",
                    name,
                    _source_ref("runtime", "company_intel_json", name, path=str(company_intel_path)),
                    metadata={"lane": "companies"},
                )
        lead_companies = (company_intel.get("lead_companies") or {}).get("without_intel") or []
        for company in lead_companies[:25]:
            if company:
                add(
                    "company",
                    str(company),
                    _source_ref("runtime", "company_intel_json", str(company), path=str(company_intel_path)),
                    metadata={"lane": "companies"},
                )

    entities: list[dict[str, Any]] = []
    for group in groups.values():
        entities.append(
            {
                "entity_id": f"{group['entity_type']}:{group['canonical_key']}",
                "entity_type": group["entity_type"],
                "canonical_key": group["canonical_key"],
                "canonical_name": group["canonical_name"],
                "aliases": sorted(group["aliases"], key=str.casefold),
                "source_refs": group["source_refs"],
                "source_ref_count": len(group["source_refs"]),
                "status": group["status"],
                "metadata": group["metadata"],
            }
        )
    entity_type_order = {"project": 0, "system": 1, "company": 2, "person": 3, "topic": 4}
    entities.sort(
        key=lambda item: (
            entity_type_order.get(item["entity_type"], 9),
            -int(item.get("source_ref_count") or 0),
            item["canonical_name"].casefold(),
        )
    )
    return entities


def materialize_normalized_entities(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    snapshot: dict[str, Any] | None = None,
    event_limit: int = 50,
) -> list[dict[str, Any]]:
    config = _config(manifest)
    built = build_normalized_entities(
        manifest,
        domain_id=domain_id,
        snapshot=snapshot,
        event_limit=event_limit,
    )
    persisted = [
        upsert_normalized_entity(
            config,
            entity_type=item["entity_type"],
            canonical_key=item["canonical_key"],
            canonical_name=item["canonical_name"],
            aliases=item["aliases"],
            source_refs=item["source_refs"],
            status=item["status"],
            metadata=item.get("metadata"),
            replace_existing=True,
        )
        for item in built
    ]
    mark_missing_normalized_entities_inactive(
        config,
        active_entity_ids=[item["id"] if "id" in item else item["entity_id"] for item in persisted],
    )
    return fetch_normalized_entities(config, limit=200)


def _search_derived_hits(
    query: str,
    records: list[dict[str, Any]],
    *,
    record_kind: str,
    text_builder,
    limit: int,
) -> list[dict[str, Any]]:
    tokens = tokenize(query)
    hits: list[dict[str, Any]] = []
    for record in records:
        haystack = text_builder(record)
        score = score_text(haystack, tokens, query)
        if score <= 0:
            continue
        hit = dict(record)
        hit["record_kind"] = record_kind
        hit["score"] = score
        hits.append(hit)
    hits.sort(key=lambda item: (-item["score"], item.get("summary_text") or ""))
    return hits[:limit]


def reconstruct_timeline(
    manifest: dict[str, Any],
    *,
    timestamp: datetime,
    domain: str | None = None,
    window_hours: int = 6,
    limit: int = 3,
    detail: str = "digest",
) -> dict[str, Any]:
    config = _config(manifest)
    payload = timeline_state(
        config,
        target=timestamp,
        domain=domain,
        window_hours=window_hours,
        limit=limit,
        visibility="raw",
        detail=detail,
    )
    payload["contract_name"] = ACTIVATION_CONTRACT_NAME
    payload["contract_version"] = ACTIVATION_CONTRACT_VERSION
    payload["truth_order"] = [
        "chronicle.db",
        "chronicle event stream",
        "runtime evidence",
        "status markdown",
        "mem0 semantic recall",
    ]
    return payload


def record_event(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    *,
    append_compat: bool = True,
    dedupe: bool = False,
    dedupe_window_hours: int = 12,
    source_kind: str = "agent_command",
    imported_from: str = "chronicle.record",
) -> dict[str, Any]:
    # Pop private test/injection keys before any serialization path sees them.
    _embed_fn_override = entry.pop("_embed_fn", None)
    # Redact before anything derives from the text: hashes, embeddings, the
    # stored row, the Mem0 outbox, and compatibility ledgers.
    normalized_entry, redactions = _redact_entry(_normalize_record_entry(entry))
    from .memory import validate_entry, request_receipt, record_observation
    validate_entry(normalized_entry)
    config = _config(manifest)
    # Canonical names for the agent, project and domain; the given spellings
    # stay beside them (FR-14). A name is caller text, and an undeclared one is
    # kept as given, so every name passes the secret filter like any text.
    config.identities.canonical_entry(normalized_entry)
    for name_key in ("agent", "project", "domain", "task_id", "actor_raw", "project_raw", "domain_raw"):
        if isinstance(normalized_entry.get(name_key), str):
            normalized_entry[name_key], found = redact_value(normalized_entry[name_key])
            for kind, count in found.items():
                redactions[kind] = redactions.get(kind, 0) + count
    skip_generic_source_archives = bool(entry.get("skip_generic_source_archives"))
    resolved_mem0_status = default_mem0_status(normalized_entry, source_kind=source_kind)
    if resolved_mem0_status is not None:
        normalized_entry["mem0_status"] = resolved_mem0_status

    if source_kind == "chronicle_mcp" and not (normalized_entry.get("checkpoint") or normalized_entry.get("fact")):
        memory_guard = evaluate_memory_guard(config, normalized_entry, source_kind=source_kind)
        normalized_entry["memory_guard"] = memory_guard
        if memory_guard["verdict"] == "local_only":
            normalized_entry["mem0_status"] = "off"

    # v8: compute a stable content_hash so store_event persists it. The hash
    # omits timestamps on purpose — it represents the logical event, not the
    # exact moment of recording — and is canonicalised as sorted-keys JSON so
    # '|' inside a field cannot collide with a neighbouring field.
    content_hash = compute_event_content_hash(
        text=normalized_entry.get("text"),
        category=normalized_entry.get("category"),
        actor=normalized_entry.get("agent"),
        entity_id=normalized_entry.get("entity_id"),
        domain=normalized_entry.get("domain"),
        project=normalized_entry.get("project"),
        why=normalized_entry.get("why"),
    )
    if normalized_entry.get("task_id"):  # one task under any spelling of its id (FR-14)
        content_hash = _sha256_text(content_hash + ":" + fold(normalized_entry["task_id"]))
    if normalized_entry.get("checkpoint") or normalized_entry.get("fact"):
        content_hash = _sha256_text(content_hash + json.dumps(
            [normalized_entry.get("checkpoint"), normalized_entry.get("fact")],
            ensure_ascii=False, sort_keys=True,
        ))
    normalized_entry["content_hash"] = content_hash

    hash_dedup_active = (
        not dedupe and feature_enabled(ENV_FEATURE_EVENT_HASH_DEDUP)
    )
    hash_dedup_window = (
        env_int(ENV_EVENT_DEDUP_WINDOW_HOURS, default=DEFAULT_EVENT_DEDUP_WINDOW_HOURS)
        if hash_dedup_active
        else 0
    )

    # Best-effort embedding, computed BEFORE the write transaction — the
    # Ollama call can take seconds and must not hold the write lock. The vector
    # is written after the event commits, in its own transaction; if that
    # fails, the event stays without one until embed-backfill repairs it.
    # Failure MUST NOT surface to the caller; the flag (default ON) lets tests
    # opt out via the env var.
    embedding_vec: list[float] | None = None
    embed_model: str | None = None
    embed_dim: int | None = None
    if feature_enabled(ENV_FEATURE_EVENT_EMBEDDINGS, default=True):
        try:
            from .embeddings import active_profile, embed_document, event_embedding_text
            # _embed_fn_override is popped from entry before normalization
            # so it never reaches payload_json.
            _embed_fn = _embed_fn_override or embed_document
            embed_input = event_embedding_text(normalized_entry)
            if embed_input:
                embedding_vec = _embed_fn(embed_input)
                if embedding_vec is not None:
                    embed_model, embed_dim = active_profile().key, len(embedding_vec)
        except Exception:  # noqa: BLE001
            embedding_vec = None  # embedding is non-critical

    # Stage attachments (hash + content-addressed copy) BEFORE the transaction:
    # copying a 25MB file must not hold SQLite's single write lock. Staging is
    # idempotent, so a rollback leaves at most an unreferenced blob.
    staged_artifacts: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    if not skip_generic_source_archives:
        for source_file in entry.get("source_files") or []:
            path = Path(source_file).expanduser()
            staged = stage_artifact_from_path(
                config,
                source_path=path,
                artifact_type="event-source",
                metadata={"category": normalized_entry.get("category")},
            )
            if staged is not None:
                staged_artifacts.append(staged)
                evidence.append({"path": str(path), "status": staged["metadata"].get("storage_mode", "archived"), "sha256": staged["sha256"]})
                if staged["metadata"].get("redactions"):
                    evidence[-1]["redactions"] = staged["metadata"]["redactions"]
            else:
                evidence.append({"path": str(path), "status": "missing" if not path.is_file() else "skipped"})

    # One BEGIN IMMEDIATE transaction for dedup lookup + event + artifacts +
    # links: a crash mid-way can no longer leave a half-recorded event, and the
    # up-front write lock avoids the deferred-BEGIN upgrade deadlock that
    # bypasses busy_timeout. The vector is written after it commits.
    artifacts_written = 0
    with write_transaction(config) as connection:
        prior_request = request_receipt(connection, normalized_entry)
        if prior_request is not None:
            previous = fetch_event(config, event_id=prior_request["event_id"], connection=connection)
            return {**previous, "chronicle_status": "existing", "chronicle_db_path": str(config.db_path),
                    "chronicle_error": None, "artifacts_written": 0, **({"redactions": redactions} if redactions else {}),
                    "observation_id": prior_request["observation_id"], "fact_id": prior_request["fact_id"],
                    "evidence": prior_request["evidence"], "dedupe_status": "request_id_match"}
        existing = None
        if dedupe:
            existing = _find_recent_exact_duplicate(config, normalized_entry,
                window_hours=dedupe_window_hours, connection=connection)
        elif hash_dedup_active:
            existing_row = find_event_by_content_hash_recent(config, content_hash=content_hash,
                window_hours=hash_dedup_window, connection=connection)
            if existing_row is not None:
                existing = fetch_event(config, event_id=existing_row["id"], connection=connection)
        stored = existing or store_event(config, normalized_entry, source_kind=source_kind,
                                        imported_from=imported_from, connection=connection)
        if existing:
            stored["dedupe_status"] = "exact_duplicate" if dedupe else "content_hash_match"
            stored["dedupe_window_hours"] = dedupe_window_hours if dedupe else hash_dedup_window
        for staged in staged_artifacts:
            staged["observed_at"] = stored["recorded_at"]
            staged["metadata"]["event_id"] = stored["id"]
            artifact = persist_staged_artifact(
                connection,
                staged,
                entity_id=stored.get("entity_id"),
            )
            link_artifact(
                config,
                artifact_id=artifact["id"],
                target_type="event",
                target_id=stored["id"],
                link_role="source",
                metadata={"path": str(staged["source_path"])},
                connection=connection,
            )
            artifacts_written += 1
        observation = record_observation(connection, normalized_entry, stored["id"], evidence, config.identities)
    if embedding_vec is not None and embed_model is not None and embed_dim is not None:
        # A vector is an index entry, not part of the record. Written in its own
        # transaction, no failure of it can lose the event, not even one that
        # makes SQLite roll back a whole transaction (a full disk); embed-backfill
        # repairs the gap.
        # Insert only: replacing a vector is embed-backfill's job, and a capture
        # racing a backfill must not put back a vector of a stale dimension.
        try:
            store_event_embedding(config, stored["id"], embedding_vec, embed_model, embed_dim, replace=False)
        except (ValueError, sqlite3.Error):
            pass
    stored["observation_id"] = observation["observation_id"]
    stored["fact_id"] = observation["fact_id"]
    stored["evidence"] = evidence
    stored["chronicle_status"] = "existing" if existing else "stored"
    stored["chronicle_db_path"] = str(config.db_path)
    stored["chronicle_error"] = None
    category_fallback = normalized_entry.get("category_fallback")
    if category_fallback:
        stored["category_fallback"] = category_fallback

    # The JSONL ledger is optional: a manifest without paths.ledger_file keeps none.
    if append_compat and not existing and (manifest.get("paths") or {}).get("ledger_file"):
        ledger_row = dict(stored)
        ledger_row["source_files"] = entry.get("source_files") or []
        ledger_row["mem0_status"] = entry.get("mem0_status")
        ledger_row["mem0_error"] = normalized_entry.get("mem0_error")
        ledger_row["mem0_raw"] = normalized_entry.get("mem0_raw")
        try:
            append_jsonl(_compat_path(manifest, "ledger_file"), ledger_row)
        except OSError as exc:
            # The canonical event is already committed. Raising here would tell
            # the caller the write failed and invite a retry that dedupes into a
            # no-op, so the ledger would never be repaired and the caller would
            # believe nothing was recorded. Report it as a partial instead.
            stored["compat_ledger_error"] = f"{type(exc).__name__}: {exc}"

    stored["artifacts_written"] = artifacts_written
    if redactions:
        stored["redactions"] = redactions
    return stored


def guard_event(
    manifest: dict[str, Any],
    *,
    event_id: str,
    verdict: str,
    reason: str,
    apply: bool = False,
) -> dict[str, Any]:
    normalized_verdict = str(verdict).strip().casefold()
    if normalized_verdict not in MEMORY_GUARD_VERDICTS - {"durable"}:
        raise ValueError("V1 supports only the `local_only` verdict.")
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("A guard reason is required.")

    config = _config(manifest)
    event = fetch_event(config, event_id=event_id)
    if event is None:
        raise ValueError(f"Unknown event id: {event_id}")

    memory_guard = {
        "version": MEMORY_GUARD_VERSION,
        "verdict": "local_only",
        "visibility": "raw_only",
        "score": 2,
        "signals": [{"name": "manual_operator_guard", "kind": "risk", "weight": 2}],
        "source": "manual_operator_guard",
        "reason": normalized_reason,
        "evaluated_at_utc": utc_now(),
    }
    payload = {
        "status": "dry_run" if not apply else "ok",
        "event_id": event_id,
        "verdict": "local_only",
        "memory_guard": memory_guard,
        "current_event": event,
    }
    if not apply:
        return payload

    updated = update_event_memory_guard(config, event_id=event_id, memory_guard=memory_guard)
    if not updated:
        raise ValueError(f"Failed to update event payload for {event_id}.")
    update_event_mem0_state(
        config,
        event_id=event_id,
        mem0_status="off",
        mem0_error=None,
        attempted=False,
    )
    payload["event"] = fetch_event(config, event_id=event_id)
    return payload


def repair_stale_runs(
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
    ttl_hours: float | int = DEFAULT_STALE_RUN_TTL_HOURS,
) -> dict[str, Any]:
    config = _config(manifest)
    result = mark_stale_running_runs(config, ttl_hours=ttl_hours, dry_run=dry_run)
    return {
        "status": "dry_run" if dry_run else "ok",
        "db_path": str(config.db_path),
    } | result


def repair_event_categories(
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = _config(manifest)
    repaired: list[dict[str, Any]] = []
    with open_connection(config) as connection, connection:
        rows = connection.execute(
            """
            SELECT id, category, event_type, payload_json
            FROM events
            WHERE category IS NULL OR TRIM(category) = ''
            ORDER BY occurred_at_utc, id
            """
        ).fetchall()
        for row in rows:
            payload = _json_loads(row["payload_json"])
            normalized_category = _normalize_category(row["category"] or payload.get("category"))
            normalized_event_type = row["event_type"] or f"agent.{normalized_category}"
            repaired.append(
                {
                    "id": row["id"],
                    "previous_category": row["category"],
                    "category": normalized_category,
                    "event_type": normalized_event_type,
                }
            )
            if dry_run:
                continue

            payload["category"] = normalized_category
            payload["event_type"] = payload.get("event_type") or normalized_event_type
            connection.execute(
                """
                UPDATE events
                SET category = ?, event_type = ?, payload_json = ?
                WHERE id = ?
                """,
                (
                    normalized_category,
                    normalized_event_type,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    row["id"],
                ),
            )

            outbox_row = connection.execute(
                """
                SELECT payload_json
                FROM mem0_outbox
                WHERE event_id = ?
                LIMIT 1
                """,
                (row["id"],),
            ).fetchone()
            if outbox_row is None:
                continue
            outbox_payload = _json_loads(outbox_row["payload_json"])
            outbox_payload["category"] = normalized_category
            connection.execute(
                """
                UPDATE mem0_outbox
                SET payload_json = ?
                WHERE event_id = ?
                """,
                (
                    json.dumps(outbox_payload, ensure_ascii=False, sort_keys=True),
                    row["id"],
                ),
            )

    return {
        "status": "dry_run" if dry_run else "ok",
        "db_path": str(config.db_path),
        "repaired_count": len(repaired),
        "repaired": repaired,
    }


def repair_mem0_state(
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = _config(manifest)
    repaired: list[dict[str, Any]] = []
    outbox_to_event = {
        "synced": "stored",
        "pending": "queued",
        "failed": "failed",
        "skipped": "skipped",
    }

    with open_connection(config) as connection, connection:
        rows = connection.execute(
            """
            SELECT
                e.id,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at_utc
            FROM events AS e
            JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            ORDER BY e.occurred_at_utc ASC, e.id ASC
            """
        ).fetchall()

        for row in rows:
            payload = _json_loads(row["payload_json"])
            outbox_payload = _json_loads(row["outbox_payload_json"])
            desired_status = outbox_to_event.get(row["outbox_status"], row["outbox_status"])
            desired_error = row["outbox_last_error"] if row["outbox_last_error"] is not None else outbox_payload.get("mem0_error")
            desired_raw = outbox_payload.get("mem0_raw")
            desired_synced_at = row["outbox_synced_at_utc"] or outbox_payload.get("mem0_synced_at")
            current_status = row["mem0_status"] or payload.get("mem0_status")
            current_error = row["mem0_error"] if row["mem0_error"] is not None else payload.get("mem0_error")
            current_raw = payload.get("mem0_raw")
            current_synced_at = payload.get("mem0_synced_at")

            if (
                current_status == desired_status
                and current_error == desired_error
                and current_raw == desired_raw
                and current_synced_at == desired_synced_at
            ):
                continue

            repaired.append(
                {
                    "id": row["id"],
                    "event_mem0_status": current_status,
                    "outbox_status": row["outbox_status"],
                    "desired_mem0_status": desired_status,
                    "event_mem0_error": current_error,
                    "desired_mem0_error": desired_error,
                    "event_mem0_raw": current_raw,
                    "desired_mem0_raw": desired_raw,
                    "event_mem0_synced_at": current_synced_at,
                    "desired_mem0_synced_at": desired_synced_at,
                }
            )
            if dry_run:
                continue

            payload["mem0_status"] = desired_status
            if desired_error is None:
                payload.pop("mem0_error", None)
            else:
                payload["mem0_error"] = desired_error
            if desired_raw is None:
                payload.pop("mem0_raw", None)
            else:
                payload["mem0_raw"] = desired_raw
            if desired_synced_at is None:
                payload.pop("mem0_synced_at", None)
            else:
                payload["mem0_synced_at"] = desired_synced_at

            connection.execute(
                """
                UPDATE events
                SET mem0_status = ?, mem0_error = ?, payload_json = ?
                WHERE id = ?
                """,
                (
                    desired_status,
                    desired_error,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    row["id"],
                ),
            )

    return {
        "status": "dry_run" if dry_run else "ok",
        "db_path": str(config.db_path),
        "repaired_count": len(repaired),
        "repaired": repaired,
    }


def backfill_mem0_queue(
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = _config(manifest)
    requeued: list[dict[str, Any]] = []

    with open_connection(config) as connection, connection:
        rows = connection.execute(
            """
            SELECT
                e.id,
                e.recorded_at_utc,
                e.category,
                e.title,
                e.source_kind,
                e.mem0_status,
                e.payload_json,
                o.status AS outbox_status
            FROM events AS e
            JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            ORDER BY e.occurred_at_utc ASC, e.id ASC
            """
        ).fetchall()

        for row in rows:
            payload = _json_loads(row["payload_json"])
            memory_guard = payload.get("memory_guard")
            if isinstance(memory_guard, dict) and memory_guard.get("verdict") == "local_only":
                continue
            current_status = row["mem0_status"] or payload.get("mem0_status")
            desired_status = default_mem0_status(
                {"category": row["category"], "mem0_status": "auto"},
                source_kind=row["source_kind"] or "",
            )
            if desired_status != "queued":
                continue
            if current_status not in {None, "", "off", "skipped"}:
                continue
            if row["outbox_status"] != "skipped":
                continue

            requeued.append(
                {
                    "id": row["id"],
                    "recorded_at": row["recorded_at_utc"],
                    "category": row["category"],
                    "project": row["title"],
                    "source_kind": row["source_kind"],
                    "event_mem0_status": current_status,
                    "outbox_status": row["outbox_status"],
                    "desired_mem0_status": "queued",
                }
            )
            if dry_run:
                continue

            payload["mem0_status"] = "queued"
            payload.pop("mem0_error", None)
            payload.pop("mem0_raw", None)
            payload.pop("mem0_synced_at", None)

            connection.execute(
                """
                UPDATE events
                SET mem0_status = ?, mem0_error = ?, payload_json = ?
                WHERE id = ?
                """,
                (
                    "queued",
                    None,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    row["id"],
                ),
            )
            connection.execute(
                """
                UPDATE mem0_outbox
                SET status = 'pending',
                    attempts = 0,
                    last_attempt_at_utc = NULL,
                    last_error = NULL,
                    synced_at_utc = NULL
                WHERE event_id = ? AND operation = 'add'
                """,
                (row["id"],),
            )

    return {
        "status": "dry_run" if dry_run else "ok",
        "db_path": str(config.db_path),
        "requeued_count": len(requeued),
        "requeued": requeued,
    }


def _snapshot_artifact_specs(manifest: dict[str, Any], snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    def add(path_str: str | None, artifact_type: str, entity_type: str | None, entity_name: str | None) -> None:
        if not path_str:
            return
        path = Path(path_str).expanduser()
        key = str(path)
        if key in seen_paths:
            return
        seen_paths.add(key)
        specs.append(
            {
                "path": path,
                "artifact_type": artifact_type,
                "entity_type": entity_type,
                "entity_name": entity_name,
            }
        )

    for item in snapshot.get("source_excerpts", []):
        add(item.get("path"), "ssot-source", None, None)

    digest = snapshot.get("digest", {})
    add(digest.get("status_path"), "digest-status", "system", "digest")
    add(digest.get("synthesis_path"), "digest-synthesis", "system", "digest")
    add(digest.get("previous_summary_path"), "digest-previous-summary", "system", "digest")

    portfolio = snapshot.get("portfolio_assets", {})
    add(portfolio.get("path"), "portfolio-asset-manifest", "project", "portfolio")
    add(manifest["paths"].get("portfolio_asset_manifest"), "portfolio-asset-manifest", "project", "portfolio")

    return specs


def persist_snapshot(
    manifest: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    append_compat: bool = True,
    render_generated: bool = True,
) -> dict[str, Any]:
    config = _config(manifest)
    stored = store_snapshot(config, snapshot)
    stored["chronicle_status"] = "stored"
    stored["chronicle_db_path"] = str(config.db_path)
    stored["chronicle_error"] = None

    side_effect_errors: dict[str, str] = {}
    # The JSONL copy is optional: a manifest without paths.snapshot_file keeps none.
    if append_compat and (manifest.get("paths") or {}).get("snapshot_file"):
        try:
            append_jsonl(_compat_path(manifest, "snapshot_file"), stored)
        except OSError as exc:
            side_effect_errors["compat_snapshot"] = f"{type(exc).__name__}: {exc}"

    run_id = None
    artifact_count = 0
    relation_count = 0
    normalized_entities: list[dict[str, Any]] = []
    try:
        run_id = start_ingest_run(
            config, adapter="runtime_snapshot", source_ref=stored["id"],
            metadata={"domain": stored.get("domain")},
        )
        for spec in _snapshot_artifact_specs(manifest, stored):
            artifact = store_artifact_from_path(
                config,
                source_path=spec["path"],
                artifact_type=spec["artifact_type"],
                observed_at_utc=stored["captured_at_utc"],
                entity_type=spec["entity_type"],
                entity_name=spec["entity_name"],
                metadata={"snapshot_id": stored["id"], "domain": stored.get("domain")},
            )
            if artifact is None:
                continue
            link_artifact(
                config,
                artifact_id=artifact["id"],
                target_type="snapshot",
                target_id=stored["id"],
                link_role="evidence",
                metadata={"artifact_type": spec["artifact_type"]},
            )
            artifact_count += 1

        normalized_entities = materialize_normalized_entities(
            manifest,
            domain_id=stored.get("domain") or "global",
            snapshot=stored,
            event_limit=64,
        )
        finish_ingest_run(
            config,
            run_id=run_id,
            status="ok",
            items_seen=len(_snapshot_artifact_specs(manifest, stored)),
            items_written=artifact_count,
            metadata={
                "relation_count": relation_count,
                "normalized_entity_count": len(normalized_entities),
            },
        )
    except Exception as exc:
        side_effect_errors["evidence_ingest"] = f"{type(exc).__name__}: {exc}"
        if run_id is not None:
            try:
                finish_ingest_run(config, run_id=run_id, status="failed",
                                  items_seen=artifact_count, items_written=artifact_count,
                                  error_text=str(exc))
            except Exception as bookkeeping_exc:
                side_effect_errors["ingest_bookkeeping"] = f"{type(bookkeeping_exc).__name__}: {bookkeeping_exc}"

    projections = []
    if render_generated:
        preferred_snapshot = stored if stored.get("domain") == "global" else None
        try:
            projections = render_projections(manifest, snapshot=preferred_snapshot)
        except Exception as exc:
            side_effect_errors["projections"] = f"{type(exc).__name__}: {exc}"

    stored["artifacts_written"] = artifact_count
    stored["relations_written"] = relation_count
    stored["normalized_entities"] = normalized_entities
    stored["projection_runs"] = projections
    stored["side_effect_errors"] = side_effect_errors
    return stored


def capture_runtime_snapshot(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    agent: str = "codex",
    title: str | None = None,
    focus: str | None = None,
    append_compat: bool = True,
    render_generated: bool = True,
) -> dict[str, Any]:
    domain_id = _domain_id(manifest, domain_id)
    config = _config(manifest)
    recent_events = fetch_recent_events(config, limit=8, domain=domain_id)
    if not recent_events:
        recent_events = _filter_events_by_visibility(sorted(
            read_jsonl(_compat_path(manifest, "ledger_file")),
            key=lambda row: row.get("recorded_at", ""),
            reverse=True,
        )[:8])
    snapshot = build_runtime_snapshot(
        manifest,
        domain_id=domain_id,
        agent=agent,
        title=title,
        focus=focus,
        recent_events=recent_events,
    )
    return persist_snapshot(
        manifest,
        snapshot,
        append_compat=append_compat,
        render_generated=render_generated,
    )


def build_activation(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    agent: str = "codex",
    title: str | None = None,
    focus: str | None = None,
    capture: bool = True,
) -> dict[str, Any]:
    domain_id = _domain_id(manifest, domain_id)
    attach_bundle = build_attach_bundle(
        manifest,
        domain_id=domain_id,
        agent=agent,
        title=title,
        focus=focus,
        capture=capture,
    )
    snapshot = attach_bundle["snapshot"]
    prompt = render_activation_prompt(
            {**snapshot, "operator": snapshot.get("operator") or _operator_label(manifest)}
        )
    warnings = attach_bundle["freshness_audit"].get("warnings", [])
    if warnings:
        warning_lines = ["", "Freshness warnings:"]
        warning_lines.extend(f"- {warning}" for warning in warnings[:6])
        if len(warnings) > 6:
            warning_lines.append(f"- Additional warnings hidden: {len(warnings) - 6}")
        prompt = f"{prompt}\n" + "\n".join(warning_lines)

    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "snapshot": snapshot,
        "prompt": prompt,
        "attach_bundle": attach_bundle,
    }


def render_projections(
    manifest: dict[str, Any],
    *,
    snapshot: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    config = _config(manifest)
    if snapshot is None:
        snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)
    if snapshot is None:
        raise ValueError("No Chronicle snapshot available to render projections.")

    with open_connection(config) as connection:
        summary = database_summary(connection)

    global_events = fetch_recent_events(config, limit=8)
    job_events = fetch_recent_events(config, limit=8, domain="job_search")
    if not job_events:
        job_events = fetch_project_events(config, project="job-search", limit=8)

    job_path = _source_path(manifest, "job_search")
    status_path = _source_path(manifest, "status")
    # Every projection writes one projection_runs row *after* the summary above
    # was read, so the count rendered into status.md must look ahead by exactly
    # the number of projections about to run. Derived from that list, not a
    # hardcoded +2 that silently drifts when a projection is added or removed.
    projection_names = ("job-search-status", "status-generated-view")
    projected_summary = dict(summary)
    projected_summary["projection_runs"] = int(summary.get("projection_runs", 0)) + len(projection_names)

    outputs: list[dict[str, Any]] = []

    job_content = render_job_search_status(
        snapshot,
        job_events,
        note=_projection_note(manifest, "job_search"),
    )
    _atomic_write_text(job_path, job_content)
    outputs.append(
        {
            "projection_name": projection_names[0],
            "target_path": str(job_path),
            "content_sha256": _sha256_text(job_content),
        }
    )

    block = render_status_generated_block(snapshot, projected_summary, global_events)
    status_content = update_status_file(status_path, block)
    outputs.append(
        {
            "projection_name": projection_names[1],
            "target_path": str(status_path),
            "content_sha256": _sha256_text(status_content),
        }
    )

    for output in outputs:
        output["run_id"] = store_projection_run(
            config,
            projection_name=output["projection_name"],
            target_path=Path(output["target_path"]),
            snapshot_id=snapshot.get("id"),
            status="ok",
            content_sha256=output["content_sha256"],
            metadata={"snapshot_id": snapshot.get("id")},
        )
    return outputs


def current_state(
    manifest: dict[str, Any],
    *,
    domain: str = "global",
    event_limit: int = 10,
) -> dict[str, Any]:
    config = _config(manifest)
    snapshot = fetch_latest_snapshot(config, domain=domain) or fetch_latest_snapshot(config)
    events = fetch_recent_events(config, limit=event_limit, domain=domain)
    with open_connection(config) as connection:
        summary = database_summary(connection)
    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "domain": domain,
        "summary": summary,
        "latest_snapshot": snapshot,
        "recent_events": events,
        "freshness_audit": build_freshness_audit(manifest, domain_id=domain),
        "source_audit": build_sources_audit(manifest, domain_id=domain),
    }


def project_state(
    manifest: dict[str, Any],
    *,
    project: str,
    event_limit: int = 10,
) -> dict[str, Any]:
    config = _config(manifest)
    project = config.identities.project(project) or project
    entity_id = _project_entity_id(project)
    events = fetch_project_events(config, project=project, limit=event_limit)
    relations = fetch_relations_for_entity(config, entity_id=entity_id)
    snapshot = fetch_latest_snapshot(config)
    return {
        "project": project,
        "entity_id": entity_id,
        "latest_snapshot": snapshot,
        "recent_events": events,
        "relations": relations,
    }


def query_context(
    manifest: dict[str, Any],
    *,
    query: str,
    domain: str | None = None,
    limit: int = 5,
    mode: str = DEFAULT_QUERY_MODE,
) -> dict[str, Any]:
    if mode not in QUERY_MODES:
        raise ValueError(f"Unsupported query mode: {mode}")
    domain = _domain_id(manifest, domain)
    config = _config(manifest)
    chronicle_hits = search_events(config, query=query, limit=limit, domain=domain)
    for hit in chronicle_hits:
        hit["source_class"] = "canonical_event"
        hit["trust_tier"] = "canonical"
        hit["freshness_status"] = _freshness_status(hit.get("recorded_at"))
        hit["freshness_rank"] = _freshness_rank(hit.get("recorded_at"))

    source_ids = manifest["domain_map"][domain]["source_ids"] if domain else [source["id"] for source in manifest["sources"]]
    status_hits: list[dict[str, Any]] = []
    for source_id in source_ids:
        source = manifest["source_map"][source_id]
        path = expand_path(source["path"])
        if not path.exists():
            continue
        trust_tier, trust_tier_source = _resolve_source_trust_tier(source)
        freshness_thresholds = _freshness_thresholds(
            manifest,
            scope="attach_sources",
            item_id=source_id,
        )
        source_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        hits = search_source_blocks(path, query, limit)
        for hit in hits:
            hit["source_id"] = source_id
            hit["source_label"] = source["label"]
            hit["source_role"] = source.get("role")
            hit["source_priority"] = source.get("priority", 0)
            hit["source_class"] = "ssot_source"
            hit["trust_tier"] = trust_tier
            hit["trust_tier_source"] = trust_tier_source
            hit["trust_rank"] = TRUST_TIER_RANK[hit["trust_tier"]]
            hit["source_mtime"] = source_mtime
            hit["freshness_thresholds_hours"] = freshness_thresholds
            hit["freshness_status"] = _freshness_status(source_mtime, thresholds=freshness_thresholds)
            hit["freshness_rank"] = _freshness_rank(source_mtime, thresholds=freshness_thresholds)
        status_hits.extend(hits)
    status_hits.sort(
        key=lambda item: (
            -item["score"],
            -item.get("freshness_rank", 0),
            -item.get("trust_rank", 0),
            -item.get("source_priority", 0),
            item["path"],
            item["line"],
        )
    )
    status_hits = status_hits[:limit]

    mem0_hits: list[dict[str, Any]] = []
    mem0_meta = None
    mem0_path = _compat_path(manifest, "mem0_dump")
    if mode != "truth_only" and mem0_path.exists():
        mem0_hits = search_mem0_dump(mem0_path, query, limit)
        mem0_meta = {
            "path": str(mem0_path),
            "mtime": datetime.fromtimestamp(mem0_path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    mem0_thresholds = _freshness_thresholds(
        manifest,
        scope="semantic_recall",
        item_id="mem0_dump",
    )
    for hit in mem0_hits:
        hit["source_class"] = "semantic_recall"
        hit["trust_tier"] = "semantic_recall"
        hit["freshness_thresholds_hours"] = mem0_thresholds
        hit["freshness_status"] = _freshness_status((mem0_meta or {}).get("mtime"), thresholds=mem0_thresholds)
        hit["freshness_rank"] = _freshness_rank((mem0_meta or {}).get("mtime"), thresholds=mem0_thresholds)
        hit["source_mtime"] = (mem0_meta or {}).get("mtime")
        hit["source_path"] = (mem0_meta or {}).get("path")

    normalized_entities = fetch_normalized_entities(config, limit=200)
    normalized_entity_hits: list[dict[str, Any]] = []
    # Nothing writes situation models or lens runs any more; the last ones
    # are months old, so they are history, not the current interpretation (W11).
    # The key stays for clients of the contract, like scenario_hits.
    interpretation_hits: list[dict[str, Any]] = []

    if mode != "truth_only":
        normalized_entity_hits = _search_derived_hits(
            query,
            normalized_entities,
            record_kind="normalized_entity",
            text_builder=lambda item: " ".join(
                [
                    item.get("canonical_name") or "",
                    " ".join(item.get("aliases") or []),
                    json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                ]
            ),
            limit=limit,
        )

    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "query": query,
        "domain": domain,
        "query_mode": mode,
        "ranking_basis": ["text_score", "freshness", "trust", "source_priority"],
        "freshness_audit": build_freshness_audit(manifest, domain_id=domain or "global"),
        "chronicle_hits": chronicle_hits,
        # Status files and the Mem0 dump are read as they are on disk; stored
        # events already passed the secret filter, these pass it here (FR-11).
        "status_hits": redact_value(status_hits)[0],
        "normalized_entity_hits": redact_value(normalized_entity_hits)[0],
        "interpretation_hits": interpretation_hits[:limit],
        "scenario_hits": [],
        "forecast_review_hits": [],
        "briefing_hits": [],
        "mem0_dump_hits": redact_value(mem0_hits)[0],
    }


# ==========================================================================
# v8 Phase 1 surface — entity aliases, entity merge, live Mem0 search,
# entity resolution report, event content-hash. Every tool is gated behind an
# environment flag (see config.ENV_FEATURE_*) so it can be disabled without
# redeploying.
# ==========================================================================


_ALIAS_NORMALIZE_WHITESPACE = re.compile(r"\s+")


def normalize_alias_text(text: str | None) -> str:
    """Canonical alias key: NFKC + casefold + whitespace collapse.

    Used as the unique key in entity_aliases. NFKC collapses compatibility
    codepoints (fullwidth digits, ligatures); casefold is Unicode-aware
    lowercasing that handles ß → ss, cyrillic, turkish dotted i, etc.;
    the whitespace pass turns any run (tabs, newlines, double spaces) into
    a single space. Leading/trailing whitespace is stripped last.
    """
    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(text)).casefold()
    collapsed = _ALIAS_NORMALIZE_WHITESPACE.sub(" ", normalized).strip()
    return collapsed


def compute_event_content_hash(
    *,
    text: str | None,
    category: str | None = None,
    actor: str | None = None,
    entity_id: str | None = None,
    domain: str | None = None,
    project: str | None = None,
    why: str | None = None,
) -> str:
    """Stable SHA-256 for a logical event, used for content-based dedup.

    The payload is serialised as a sorted-keys JSON object so delimiter
    collisions are impossible (``("a|b","c")`` and ``("a","b|c")`` now
    produce distinct hashes) and the canonicalisation is deterministic
    across Python versions. Timestamps are excluded on purpose — two rapid
    calls with the same semantic payload collide and dedup kicks in.

    Domain/project/why are included because Chronicle routinely records
    functionally distinct events that share text+category in different
    domains (``digest_run`` across projects, audits, canaries).
    """
    canonical = json.dumps(
        {
            "text": (text or "").strip(),
            "category": (category or "").strip(),
            "actor": (actor or "").strip(),
            "entity_id": (entity_id or "").strip(),
            "domain": (domain or "").strip(),
            "project": (project or "").strip(),
            "why": (why or "").strip(),
        },
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _manifest_status_root(manifest: dict[str, Any]) -> Path:
    """Resolve status root from a loaded manifest dict."""
    raw = manifest.get("__path__") or manifest.get("__manifest_path__")
    if raw:
        return Path(raw).expanduser().resolve().parent
    cfg = _config(manifest)
    return Path(cfg.status_root)


def _resolve_mem0_bridge_path(manifest: dict[str, Any]) -> Path:
    """Locate ``scripts/mem0_bridge.py`` relative to the Chronicle status root."""
    return _manifest_status_root(manifest) / "scripts" / "mem0_bridge.py"


def add_entity_alias_service(
    manifest: dict[str, Any],
    *,
    alias_text: str,
    canonical_entity_id: str,
    entity_type: str,
    domain: str = "global",
    source: str | None = "manual",
    confidence: float = 1.0,
    alias_key_kind: str = "base",
    dry_run: bool = False,
) -> dict[str, Any]:
    if not feature_enabled(ENV_FEATURE_ENTITY_ALIASES):
        return {
            "status": "disabled",
            "reason": f"{ENV_FEATURE_ENTITY_ALIASES}=off",
            "alias": None,
        }
    if not alias_text or not alias_text.strip():
        raise ValueError("alias_text is required and must not be blank")
    if not canonical_entity_id:
        raise ValueError("canonical_entity_id is required")

    key = normalize_alias_text(alias_text)
    if not key:
        raise ValueError("alias_text is empty after normalization")

    config = _config(manifest)
    outcome = add_entity_alias(
        config,
        canonical_entity_id=canonical_entity_id,
        entity_type=entity_type,
        alias_text=alias_text,
        alias_key=key,
        domain=domain,
        alias_key_kind=alias_key_kind,
        confidence=confidence,
        source=source,
        dry_run=dry_run,
    )
    outcome["status"] = outcome.get("status", "ok")
    outcome["normalized_key"] = key
    return outcome


def merge_entities_service(
    manifest: dict[str, Any],
    *,
    source_entity_id: str,
    target_entity_id: str,
    reason: str,
    actor: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if not feature_enabled(ENV_FEATURE_ENTITY_ALIASES):
        return {
            "status": "disabled",
            "reason": f"{ENV_FEATURE_ENTITY_ALIASES}=off",
        }
    if not reason or not reason.strip():
        raise ValueError("reason is required when merging entities")

    config = _config(manifest)
    summary = merge_normalized_entities(
        config,
        source_entity_id=source_entity_id,
        target_entity_id=target_entity_id,
        reason=reason,
        actor=actor,
        dry_run=dry_run,
    )
    summary["status"] = "dry_run" if dry_run else "ok"
    return summary


def entity_resolution_report_service(
    manifest: dict[str, Any],
    *,
    domain: str | None = None,
) -> dict[str, Any]:
    config = _config(manifest)
    stats = entity_alias_stats(config, domain=domain)
    stats["status"] = "ok"
    stats["feature_enabled"] = feature_enabled(ENV_FEATURE_ENTITY_ALIASES)
    return stats


def search_mem0_live_service(
    manifest: dict[str, Any],
    *,
    query: str,
    limit: int = 10,
    collection: str = "personal",
    category: str | None = None,
    timeout_s: float | None = None,
) -> dict[str, Any]:
    """Wrap ``scripts/mem0_bridge.py search --json`` as an MCP tool.

    Fail-closed: timeouts, non-zero exit, or unparseable output all return
    ``{"status": "degraded", ...}`` with ``results=[]`` — never raises, so
    callers can trust Chronicle's own answers regardless of Mem0 health.
    """
    if not feature_enabled(ENV_FEATURE_MEM0_LIVE_SEARCH):
        return {
            "status": "disabled",
            "reason": f"{ENV_FEATURE_MEM0_LIVE_SEARCH}=off",
            "results": [],
        }
    if not query or not query.strip():
        raise ValueError("query is required and must not be blank")
    if collection not in {"personal", "digest", "both"}:
        raise ValueError(f"invalid collection: {collection!r}")

    bridge_path = _resolve_mem0_bridge_path(manifest)
    if not bridge_path.exists():
        return {
            "status": "degraded",
            "reason": f"mem0_bridge not found at {bridge_path}",
            "results": [],
        }

    timeout = (
        float(timeout_s)
        if timeout_s is not None
        else env_float(ENV_MEM0_LIVE_TIMEOUT_S, default=DEFAULT_MEM0_LIVE_TIMEOUT_S)
    )
    cmd = [
        "uv", "run", str(bridge_path),
        "search", query,
        "--json",
        "--limit", str(int(limit)),
        "--collection", collection,
    ]
    if category:
        cmd.extend(["--category", category])

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "degraded",
            "reason": f"mem0 bridge timed out after {timeout}s",
            "results": [],
        }
    except FileNotFoundError as exc:
        return {
            "status": "degraded",
            "reason": f"mem0 bridge not runnable ({exc})",
            "results": [],
        }

    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip().splitlines()[-1:] if proc.stderr else []
        return {
            "status": "degraded",
            "reason": f"mem0 bridge exit={proc.returncode}: {''.join(stderr_tail)[:300]}",
            "results": [],
        }

    stdout = (proc.stdout or "").strip()
    if not stdout:
        return {"status": "degraded", "reason": "empty stdout", "results": []}

    last_line = stdout.splitlines()[-1]
    try:
        payload = json.loads(last_line)
    except json.JSONDecodeError:
        return {
            "status": "degraded",
            "reason": "unparseable JSON from mem0 bridge",
            "results": [],
        }

    payload.setdefault("status", "ok")
    payload.setdefault("results", [])
    # Mem0 holds memories written before the secret filter existed (FR-11).
    payload, redactions = redact_value(payload)
    if redactions:
        payload["redactions"] = dict(redactions)
    return payload


# ---------------------------------------------------------------------------
# Hybrid recall — query_memory (RRF-fused FTS + vector + temporal)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Embed backfill
# ---------------------------------------------------------------------------


def embed_backfill(
    manifest: dict[str, Any],
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Embed every event the active profile's index lacks.

    Returns counts: {embedded, skipped, failed}, the index's model key, and the
    model's dimension (None while the backend is unavailable).
    Best-effort — individual failures do not abort the run, but an unavailable
    backend (the dimension probe fails) ends it at once with backend_unavailable.
    """
    from .embeddings import BACKFILL_TIMEOUT_S, DIMENSION_PROBE_TEXT, active_profile, embed_document, event_embedding_text

    config = _config(manifest)
    model_key = active_profile().key
    # The model's current dimension: a stored vector of another one (the model
    # behind the key changed) is replaced. Unknown while the backend is down.
    probe = embed_document(DIMENSION_PROBE_TEXT, timeout=BACKFILL_TIMEOUT_S)
    dim = len(probe) if probe is not None else None
    event_ids = fetch_event_ids_without_embedding(config, model_key=model_key, dim=dim)
    if limit is not None:
        event_ids = event_ids[:limit]
    if probe is None:
        # The backend is down or hung: calling it once per event would only
        # repeat the timeout. Nothing was embedded; the next run retries.
        return {"model_key": model_key, "dim": None, "total_without_embedding": len(event_ids),
                "embedded": 0, "skipped": 0, "failed": len(event_ids), "backend_unavailable": True}

    embedded = 0
    skipped = 0
    failed = 0

    for event_id in event_ids:
        try:
            event = fetch_event(config, event_id=event_id)
            if event is None:
                skipped += 1
                continue
            embed_input = event_embedding_text(event)
            if not embed_input:
                skipped += 1
                continue
            vec = embed_document(embed_input, timeout=BACKFILL_TIMEOUT_S)
            if vec is None:
                failed += 1
                continue
            store_event_embedding(config, event_id, vec, model_key, len(vec))
            embedded += 1
        except Exception:  # noqa: BLE001
            failed += 1

    return {
        "model_key": model_key,
        "dim": dim,
        "total_without_embedding": len(event_ids),
        "embedded": embedded,
        "skipped": skipped,
        "failed": failed,
        "backend_unavailable": False,
    }
