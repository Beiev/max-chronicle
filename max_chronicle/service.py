from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from .config import ACTIVATION_CONTRACT_NAME, ACTIVATION_CONTRACT_VERSION
from .db import database_summary
from .projections import render_job_search_status, render_status_generated_block, update_status_file
from .runtime_context import (
    append_jsonl,
    build_runtime_snapshot,
    expand_path,
    file_meta,
    load_manifest,
    mem0_meta,
    normalize_heading,
    parse_markdown_sections,
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
    config_from_manifest,
    fetch_event,
    fetch_latest_projection_run,
    fetch_latest_snapshot,
    fetch_latest_situation_model,
    fetch_lens_runs,
    fetch_normalized_entities,
    fetch_project_events,
    fetch_recent_events,
    fetch_relations_for_entity,
    fetch_scenario_runs,
    fetch_situation_model,
    fetch_forecast_reviews,
    finish_ingest_run,
    link_artifact,
    mark_missing_normalized_entities_inactive,
    open_connection,
    search_events,
    start_ingest_run,
    store_forecast_review,
    store_lens_run,
    store_scenario_run,
    store_situation_model,
    store_artifact_from_path,
    store_event,
    store_projection_run,
    store_snapshot,
    timeline_state,
    update_event_mem0_state,
    update_event_memory_guard,
    upsert_normalized_entity,
    upsert_relation,
)


VALID_CATEGORY_RE = re.compile(r"^[a-z0-9_.-]{2,64}$")
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
MEMORY_GUARD_EVIDENCE_CATEGORIES = frozenset({"decision", "milestone", "blocker", "implementation", "architecture_insight", "workflow"})
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
FIXED_LENSES = ("operator", "strategist", "risk", "market", "systems")
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
    "openclaw_state_json": {"label": "OpenClaw State", "lane": "agents"},
    "openclaw_brief_json": {"label": "OpenClaw Morning Brief", "lane": "agents"},
    "openclaw_leads_json": {"label": "OpenClaw Leads", "lane": "career_market"},
    "digest_status_json": {"label": "Digest Status", "lane": "world"},
    "digest_synthesis_md": {"label": "Digest Synthesis", "lane": "world"},
    "digest_previous_summary": {"label": "Digest Previous Summary", "lane": "world"},
    "company_intel_json": {"label": "Company Intel", "lane": "companies"},
    "openclaw_email_triage_json": {"label": "Email Triage", "lane": "life_admin"},
}


def _config(manifest: dict[str, Any]):
    return config_from_manifest(manifest)


def _compat_path(manifest: dict[str, Any], key: str) -> Path:
    return expand_path(manifest["paths"][key])


def _source_path(manifest: dict[str, Any], source_id: str) -> Path:
    return expand_path(manifest["source_map"][source_id]["path"])


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


def _normalize_category(value: str | None) -> str:
    if value is None:
        return "note"
    normalized = re.sub(r"[\s-]+", "_", value.strip().casefold())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return "note"
    normalized = CATEGORY_ALIASES.get(normalized, normalized)
    _validate_category(normalized)
    if normalized not in CANONICAL_EVENT_CATEGORIES:
        raise ValueError(
            f"Unsupported category: {normalized}. Allowed categories: {', '.join(sorted(CANONICAL_EVENT_CATEGORIES))}"
        )
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


def _normalize_record_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(entry)
    normalized["domain"] = str(normalized.get("domain") or "global").strip() or "global"
    normalized["category"] = _normalize_category(normalized.get("category"))
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
            (existing.get("domain") or "global") == entry["domain"]
            and (existing.get("category") or "note") == entry["category"]
            and (existing.get("project") or "") == (entry.get("project") or "")
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
    if fallback_filename:
        return expand_path(manifest["paths"]["openclaw_memory_dir"]) / fallback_filename
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


def build_sources_audit(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
) -> dict[str, Any]:
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
    situation_model = fetch_latest_situation_model(config, domain=domain_id, snapshot_id=snapshot.get("id"))
    if situation_model is None:
        persisted_entities = fetch_normalized_entities(config, limit=200)
        situation_model = build_situation_model(
            manifest,
            domain_id=domain_id,
            snapshot=snapshot,
            normalized_entities=persisted_entities or None,
        )
    latest_lens_runs = fetch_lens_runs(config, situation_id=situation_model["situation_id"]) if situation_model else []
    if not latest_lens_runs and situation_model is not None:
        latest_lens_runs = run_lenses(
            manifest,
            domain_id=domain_id,
            situation_model=situation_model,
            persist=False,
        )
    latest_scenarios = fetch_scenario_runs(config, situation_id=situation_model["situation_id"], limit=8) if situation_model else []
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
        "situation_model": situation_model,
        "latest_lens_runs": latest_lens_runs,
        "latest_scenarios": latest_scenarios,
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
    openclaw = snapshot.get("openclaw", {})
    digest = snapshot.get("digest", {})
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
        "openclaw": {
            "last_scout": openclaw.get("last_scout"),
            "last_nightly": openclaw.get("last_nightly"),
            "jobs_found_today": openclaw.get("jobs_found_today", 0),
            "applications_sent_today": openclaw.get("applications_sent_today", 0),
            "gpt_calls_today": openclaw.get("gpt_calls_today", 0),
            "pipeline_counts": openclaw.get("pipeline_counts", {}),
            "top_leads": openclaw.get("top_leads", [])[:3],
            "lead_count": openclaw.get("lead_count", 0),
        },
        "digest": {
            "status": digest.get("status", {}),
            "world_headlines": digest.get("world_headlines", [])[:4],
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


def _build_startup_focus(
    situation_model: dict[str, Any] | None,
    latest_lens_runs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Build compact focus section from situation model and lens runs."""
    if situation_model is None:
        return None
    return {
        "situation_id": situation_model.get("situation_id"),
        "summary_text": situation_model.get("summary_text", ""),
        "goals": situation_model.get("goals", [])[:5],
        "constraints": situation_model.get("constraints", [])[:6],
        "pressures": situation_model.get("pressures", [])[:6],
        "open_questions": situation_model.get("open_questions", [])[:5],
        "risks": situation_model.get("risks", [])[:4],
        "trajectory": situation_model.get("trajectory"),
        "lens_summaries": [
            {
                "lens": run.get("lens"),
                "summary_text": run.get("summary_text"),
                "confidence": run.get("confidence"),
            }
            for run in latest_lens_runs
        ],
    }


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
    if not recent_events:
        recent_events = _filter_events_by_visibility(
            snapshot.get("recent_ledger", [])[:limit]
        )

    # 3. Build enriched sources (full content or truncated)
    sources = _build_startup_sources(manifest, domain_id, compact=compact)

    # 4. Freshness audit → compact source health
    freshness_audit = build_freshness_audit(manifest, domain_id=domain_id)
    source_health = _build_startup_source_health(freshness_audit, compact=compact)

    # 5. Runtime view (compact extract from snapshot)
    runtime = _build_startup_runtime_view(snapshot)

    # 6. Situation model + lens runs → focus
    situation_model = fetch_latest_situation_model(
        config, domain=domain_id, snapshot_id=snapshot.get("id")
    )
    if situation_model is None:
        persisted_entities = fetch_normalized_entities(config, limit=200)
        situation_model = build_situation_model(
            manifest,
            domain_id=domain_id,
            snapshot=snapshot,
            normalized_entities=persisted_entities or None,
        )
    latest_lens_runs = (
        fetch_lens_runs(config, situation_id=situation_model["situation_id"])
        if situation_model
        else []
    )
    if not latest_lens_runs and situation_model is not None:
        latest_lens_runs = run_lenses(
            manifest,
            domain_id=domain_id,
            situation_model=situation_model,
            persist=False,
        )
    focus_section = _build_startup_focus(situation_model, latest_lens_runs)

    # 7. Normalized entities → compact digest
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

    # 8. Mem0 hits (from snapshot, no duplication)
    mem0_dump_hits = snapshot.get("mem0_snapshot_hits", {})

    # 9. DB summary
    with open_connection(config) as connection:
        db_summary = database_summary(connection)

    domain_config = manifest["domain_map"][domain_id]
    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "startup_bundle_schema_version": STARTUP_BUNDLE_SCHEMA_VERSION,
        "generated_at": snapshot.get("captured_at_utc"),
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
        "focus": focus_section,
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


def _summary_lines_from_excerpt(text: str, *, limit: int = 5) -> list[str]:
    def clean(value: str) -> str:
        value = re.sub(r"`([^`]+)`", r"\1", value)
        value = re.sub(r"\*\*([^*]+)\*\*", r"\1", value)
        value = re.sub(r"__([^_]+)__", r"\1", value)
        return re.sub(r"\s+", " ", value).strip()

    numbered: list[str] = []
    lines: list[str] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if re.match(r"^\d+\.\s+", stripped):
            numbered.append(clean(re.sub(r"^\d+\.\s+", "", stripped)))
        elif stripped.startswith("- "):
            lines.append(clean(stripped[2:].strip()))
        elif stripped.endswith("?"):
            lines.append(clean(stripped))
        if len(numbered) >= limit:
            break
        if len(lines) >= limit:
            break
    return (numbered or lines)[:limit]


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


def _dedupe_text(values: list[str], *, limit: int) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in values:
        cleaned = _clean_markdown_text(item)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
        if len(deduped) >= limit:
            break
    return deduped


def _extract_priority_goals(text: str, *, limit: int) -> list[str]:
    sections = parse_markdown_sections(text)
    priority_section = next(
        (section for section in sections if section["normalized"] == normalize_heading("Priority Stack (ordered)")),
        None,
    )
    section_text = priority_section["body"] if priority_section else text

    goals: list[str] = []
    current_heading: str | None = None
    current_target: str | None = None

    def flush() -> None:
        if not current_heading:
            return
        goal = f"{current_heading}: {current_target}" if current_target else current_heading
        goals.append(shorten(_clean_markdown_text(goal), 180))

    for raw_line in section_text.splitlines():
        stripped = raw_line.strip()
        heading_match = re.match(r"^###\s+(.*)$", stripped)
        if heading_match:
            flush()
            title = _clean_markdown_text(heading_match.group(1))
            title = re.sub(r"^[^A-Za-zА-Яа-я0-9]+", "", title)
            title = re.sub(r"^P\d+\s*[—-]\s*", "", title, flags=re.IGNORECASE)
            current_heading = title or None
            current_target = None
            continue
        label, value = _parse_labeled_bullet(stripped)
        if current_heading and label == "target" and value and current_target is None:
            current_target = value

    flush()
    return _dedupe_text(goals, limit=limit)


def _section_bullets(text: str, titles: set[str], *, limit: int) -> list[str]:
    values: list[str] = []
    wanted = {normalize_heading(title) for title in titles}
    for section in parse_markdown_sections(text):
        if section["normalized"] not in wanted:
            continue
        for raw_line in section["body"].splitlines():
            stripped = raw_line.strip()
            if not stripped.startswith("- "):
                continue
            label, value = _parse_labeled_bullet(stripped)
            if value:
                values.append(value)
            else:
                values.append(_clean_markdown_text(stripped[2:]))
            if len(values) >= limit:
                return _dedupe_text(values, limit=limit)
    return _dedupe_text(values, limit=limit)


def _extract_priority_constraints(text: str, *, limit: int) -> list[str]:
    constraints: list[str] = []
    for raw_line in text.splitlines():
        label, value = _parse_labeled_bullet(raw_line.strip())
        if label in {"rule", "depends on", "portfolio dependency", "constraint"} and value:
            constraints.append(value)
    constraints.extend(
        _section_bullets(
            text,
            {"Anti-Patterns (ADHD Guard Rails)", "Key Constraints"},
            limit=limit,
        )
    )
    return _dedupe_text(constraints, limit=limit)


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


def _entity_quality_score(entity: dict[str, Any]) -> tuple[int, int]:
    aliases = entity.get("aliases") or []
    canonical_name = (entity.get("canonical_name") or "").casefold()
    score = len(entity.get("source_refs") or []) if "source_refs" in entity else int(entity.get("source_ref_count") or 0)
    if canonical_name in {"business", "cloud", "marketing", "sr"}:
        score -= 3
    if "..." in canonical_name or "key" in canonical_name:
        score -= 4
    if ".com" in canonical_name or "remote" in canonical_name:
        score -= 1
    if len(canonical_name) <= 2:
        score -= 3
    if len(aliases) > 1:
        score += 1
    return score, len(aliases)


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

    for system_name in ("chronicle", "openclaw", "digest", "mem0"):
        if system_name in {"openclaw", "mem0"} and not agents_enabled:
            continue
        if system_name == "digest" and not world_enabled:
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
                    if system_name in {"openclaw", "mem0"}
                    else "world"
                    if system_name == "digest"
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

        openclaw = resolved_snapshot.get("openclaw") or {}
        if companies_enabled:
            for lead in openclaw.get("top_leads", [])[:12]:
                company = lead.get("company")
                if company:
                    add(
                        "company",
                        company,
                        _source_ref("snapshot", resolved_snapshot["id"], company, path=openclaw.get("leads_path")),
                        metadata={"lane": "companies"},
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

    openclaw = resolved_snapshot.get("openclaw") if resolved_snapshot else {}
    leads_path = openclaw.get("leads_path") if isinstance(openclaw, dict) else None
    if not leads_path:
        fallback_leads_path = _optional_runtime_path(manifest, "openclaw_leads_json")
        leads_path = str(fallback_leads_path) if fallback_leads_path else None
    if companies_enabled and leads_path:
        leads_payload = read_json(Path(leads_path)) or []
        if isinstance(leads_payload, list):
            for item in leads_payload[:100]:
                company = item.get("company") if isinstance(item, dict) else None
                if company:
                    add(
                        "company",
                        company,
                        _source_ref("runtime", "openclaw_leads_json", company, path=leads_path),
                        metadata={"lane": "companies"},
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


def _event_transition_label(event: dict[str, Any]) -> str:
    category = event.get("category") or "note"
    project = event.get("project") or event.get("entity_id") or "system"
    return f"{category}:{project}"


def _detect_contradictions(
    recent_events: list[dict[str, Any]],
    normalized_entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    contradictions: list[dict[str, Any]] = []
    status_by_project: dict[str, set[str]] = {}
    for event in recent_events:
        project = event.get("project")
        if not project:
            continue
        statuses = status_by_project.setdefault(project, set())
        if event.get("category") == "blocker":
            statuses.add("blocked")
        if event.get("category") in {"milestone", "implementation", "state_change"}:
            statuses.add("advancing")
    for project, statuses in status_by_project.items():
        if {"blocked", "advancing"} <= statuses:
            contradictions.append(
                {
                    "entity_id": f"project:{project.casefold().replace(' ', '-')}",
                    "detail": f"Project `{project}` has both blocker and progress signals in the recent event window.",
                    "evidence_refs": [event["id"] for event in recent_events if event.get("project") == project][:6],
                }
            )

    for entity in normalized_entities:
        aliases = {alias.casefold() for alias in entity.get("aliases", [])}
        if _has_noisy_alias(entity):
            contradictions.append(
                {
                    "entity_id": entity.get("entity_id") or entity.get("id"),
                    "detail": f"Entity `{entity['canonical_name']}` still has noisy aliases that need cleanup.",
                    "evidence_refs": [ref.get("source_id") for ref in entity.get("source_refs", [])[:4]],
                }
            )
    return contradictions


def _situation_summary(
    domain_id: str,
    *,
    goals: list[str],
    pressures: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
    risks: list[dict[str, Any]],
) -> str:
    goal_text = goals[0] if goals else f"{domain_id} state is active"
    pressure_text = pressures[0]["detail"] if pressures else "no major external pressure captured"
    contradiction_text = (
        f"{len(contradictions)} contradiction(s) active"
        if contradictions
        else "no active contradiction detected"
    )
    risk_text = risks[0]["detail"] if risks else "no elevated risk captured"
    return f"{goal_text}; pressure: {pressure_text}; {contradiction_text}; risk: {risk_text}."


def build_situation_model(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    snapshot: dict[str, Any] | None = None,
    event_limit: int = 24,
    normalized_entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    config = _config(manifest)
    resolved_snapshot = snapshot or fetch_latest_snapshot(config, domain=domain_id) or fetch_latest_snapshot(config)
    recent_events = fetch_recent_events(config, limit=event_limit, domain=domain_id)
    resolved_entities = normalized_entities or fetch_normalized_entities(config, limit=200) or build_normalized_entities(
        manifest,
        domain_id=domain_id,
        snapshot=resolved_snapshot,
        event_limit=max(event_limit, 50),
    )
    source_audit = build_sources_audit(manifest, domain_id=domain_id)
    source_excerpt_map = {
        item["id"]: item.get("excerpt", "")
        for item in (resolved_snapshot or {}).get("source_excerpts", [])
    }
    priorities_content = ""
    if "priorities" in manifest.get("source_map", {}):
        priorities_content = read_source_content(manifest["source_map"]["priorities"]).get("content", "")
    status_content = ""
    if "status" in manifest.get("source_map", {}):
        status_content = read_source_content(manifest["source_map"]["status"]).get("content", "")

    goals = _extract_priority_goals(priorities_content, limit=5)
    if not goals:
        goals = _summary_lines_from_excerpt(source_excerpt_map.get("priorities", ""), limit=5)
    if not goals:
        goals = [event["text"] for event in recent_events if event.get("category") in {"decision", "milestone"}][:5]
    goals = [shorten(goal, 180) for goal in goals]

    constraints = _extract_priority_constraints(priorities_content, limit=8)
    if not constraints:
        constraints = _summary_lines_from_excerpt(source_excerpt_map.get("priorities", ""), limit=6)
    if not constraints:
        constraints = _summary_lines_from_excerpt(status_content or source_excerpt_map.get("status", ""), limit=6)
    constraints = [shorten(item, 180) for item in constraints]

    pressures: list[dict[str, Any]] = []
    for issue in source_audit["freshness_audit"].get("issues", [])[:8]:
        pressures.append(
            {
                "kind": issue["kind"],
                "detail": issue["detail"],
                "severity": issue["severity"],
                "source_id": issue.get("source_id"),
            }
        )

    if resolved_snapshot:
        portfolio = resolved_snapshot.get("portfolio_assets") or {}
        if portfolio.get("missing_assets_total", 0):
            pressures.append(
                {
                    "kind": "portfolio_gap",
                    "detail": f"Portfolio still has {portfolio.get('missing_assets_total')} missing asset slot(s).",
                    "severity": "warn",
                    "source_id": "portfolio_asset_manifest",
                }
            )
        openclaw = resolved_snapshot.get("openclaw") or {}
        if openclaw.get("gpt_limit_warning"):
            pressures.append(
                {
                    "kind": "openclaw_limit",
                    "detail": str(openclaw["gpt_limit_warning"]),
                    "severity": "warn",
                    "source_id": "openclaw_state_json",
                }
            )
        if int(openclaw.get("applications_sent_today") or 0) == 0 and int(openclaw.get("jobs_found_today") or 0) > 0:
            pressures.append(
                {
                    "kind": "pipeline_conversion",
                    "detail": "Job search is generating leads today without same-day application conversion.",
                    "severity": "warn",
                    "source_id": "openclaw_state_json",
                }
            )

    contradictions = _detect_contradictions(recent_events, resolved_entities)
    open_questions = [
        f"How should `{issue.get('source_id')}` be refreshed or trusted?"
        for issue in source_audit["freshness_audit"].get("issues", [])
        if issue.get("severity") == "critical"
    ][:5]
    open_questions.extend(
        shorten(event["text"], 220)
        for event in recent_events
        if event.get("category") == "research"
    )
    open_questions = open_questions[:6]

    risks: list[dict[str, Any]] = []
    for contradiction in contradictions[:5]:
        risks.append(
            {
                "kind": "contradiction",
                "detail": contradiction["detail"],
                "evidence_refs": contradiction["evidence_refs"],
            }
        )
    for issue in source_audit["freshness_audit"].get("issues", []):
        if issue.get("severity") == "critical":
            risks.append(
                {
                    "kind": issue["kind"],
                    "detail": issue["detail"],
                    "evidence_refs": [issue.get("source_id")],
                }
            )
    risks = risks[:8]

    actors = []
    actor_quotas = {"project": 8, "system": 2, "company": 4}
    for actor_type in ("project", "system", "company"):
        candidates = [entity for entity in resolved_entities if entity["entity_type"] == actor_type]
        candidates.sort(
            key=lambda entity: (
                -_entity_quality_score(entity)[0],
                -_entity_quality_score(entity)[1],
                entity["canonical_name"].casefold(),
            )
        )
        added_count = 0
        for entity in candidates:
            if actor_type == "company" and _entity_quality_score(entity)[0] <= 0:
                continue
            actors.append(
                {
                    "entity_id": entity["id"] if "id" in entity else entity["entity_id"],
                    "entity_type": entity["entity_type"],
                    "canonical_name": entity["canonical_name"],
                    "aliases": entity["aliases"][:5],
                }
            )
            added_count += 1
            if added_count >= actor_quotas[actor_type]:
                break

    trajectory = {
        "recent_transitions": [_event_transition_label(event) for event in recent_events[:8]],
        "event_categories": Counter(event.get("category") or "note" for event in recent_events),
        "top_vectors": goals[:3],
    }

    snapshot_id = resolved_snapshot.get("id") if resolved_snapshot else None
    valid_at = (resolved_snapshot or {}).get("captured_at_utc") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_text = _situation_summary(
        domain_id,
        goals=goals,
        pressures=pressures,
        contradictions=contradictions,
        risks=risks,
    )
    return {
        "situation_id": str(hashlib.sha256(f"{domain_id}:{snapshot_id or valid_at}".encode("utf-8")).hexdigest()[:24]),
        "domain": domain_id,
        "snapshot_id": snapshot_id,
        "valid_at": valid_at,
        "status": "active",
        "summary_text": summary_text,
        "actors": actors,
        "goals": goals,
        "pressures": pressures,
        "constraints": constraints[:8],
        "contradictions": contradictions,
        "open_questions": open_questions,
        "risks": risks,
        "trajectory": {
            "recent_transitions": trajectory["recent_transitions"],
            "event_categories": dict(trajectory["event_categories"]),
            "top_vectors": trajectory["top_vectors"],
        },
        "derived_from": {
            "snapshot_id": snapshot_id,
            "event_ids": [event["id"] for event in recent_events],
            "source_ids": [entry["source_id"] for entry in source_audit["source_catalog"] if entry.get("enabled")],
            "normalized_entity_ids": [entity["id"] if "id" in entity else entity["entity_id"] for entity in resolved_entities],
        },
    }


def materialize_situation_model(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    snapshot: dict[str, Any] | None = None,
    event_limit: int = 24,
    normalized_entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    model = build_situation_model(
        manifest,
        domain_id=domain_id,
        snapshot=snapshot,
        event_limit=event_limit,
        normalized_entities=normalized_entities,
    )
    return store_situation_model(_config(manifest), model)


def _lens_findings(situation_model: dict[str, Any], lens: str) -> tuple[list[dict[str, Any]], float]:
    findings: list[dict[str, Any]] = []
    if lens == "operator":
        if situation_model.get("goals"):
            findings.append({"title": "Current Goal", "detail": situation_model["goals"][0], "severity": "info"})
        if situation_model.get("pressures"):
            findings.append({"title": "Primary Pressure", "detail": situation_model["pressures"][0]["detail"], "severity": "warn"})
        confidence = 0.78
    elif lens == "strategist":
        vector = (situation_model.get("trajectory") or {}).get("top_vectors") or []
        findings.append(
            {
                "title": "Strategic Direction",
                "detail": vector[0] if vector else "Need clearer strategic vector from current evidence.",
                "severity": "info",
            }
        )
        if situation_model.get("constraints"):
            findings.append({"title": "Constraint", "detail": situation_model["constraints"][0], "severity": "warn"})
        confidence = 0.74
    elif lens == "risk":
        if situation_model.get("risks"):
            findings.extend(
                {
                    "title": "Risk",
                    "detail": risk["detail"],
                    "severity": "critical" if risk["kind"] == "contradiction" else "warn",
                }
                for risk in situation_model["risks"][:3]
            )
        else:
            findings.append({"title": "Risk", "detail": "No elevated risk captured from current evidence.", "severity": "info"})
        confidence = 0.82
    elif lens == "market":
        actors = [actor["canonical_name"] for actor in situation_model.get("actors", []) if actor["entity_type"] == "company"]
        findings.append(
            {
                "title": "Market Signal",
                "detail": ", ".join(actors[:3]) if actors else "No strong company/market actor signal captured.",
                "severity": "info",
            }
        )
        confidence = 0.66
    else:
        pressures = situation_model.get("pressures") or []
        findings.append(
            {
                "title": "System Health",
                "detail": pressures[0]["detail"] if pressures else "No major systems pressure captured.",
                "severity": "warn" if pressures else "info",
            }
        )
        confidence = 0.8
    return findings, confidence


def run_lenses(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    situation_model: dict[str, Any] | None = None,
    persist: bool = True,
) -> list[dict[str, Any]]:
    config = _config(manifest)
    resolved_situation = situation_model or fetch_latest_situation_model(config, domain=domain_id)
    if resolved_situation is None:
        resolved_situation = materialize_situation_model(manifest, domain_id=domain_id)
    evidence_refs = [{"type": "snapshot", "id": resolved_situation.get("snapshot_id")}]
    evidence_refs.extend({"type": "event", "id": event_id} for event_id in (resolved_situation.get("derived_from") or {}).get("event_ids", [])[:6])

    runs: list[dict[str, Any]] = []
    for lens in FIXED_LENSES:
        findings, confidence = _lens_findings(resolved_situation, lens)
        run = {
            "run_id": hashlib.sha256(f"{resolved_situation['situation_id']}:{lens}".encode("utf-8")).hexdigest()[:24],
            "situation_id": resolved_situation["situation_id"],
            "lens": lens,
            "status": "completed",
            "confidence": confidence,
            "summary_text": findings[0]["detail"] if findings else f"{lens} lens completed.",
            "findings": findings,
            "evidence_refs": evidence_refs,
        }
        runs.append(store_lens_run(config, run) if persist else run)
    return runs


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


def record_scenario(
    manifest: dict[str, Any],
    *,
    domain_id: str = "global",
    scenario_name: str,
    assumptions: list[str],
    changed_variables: dict[str, Any] | None = None,
    expected_outcomes: list[str] | None = None,
    failure_modes: list[str] | None = None,
    confidence: float = 0.6,
    review_due_at: str | None = None,
    situation_id: str | None = None,
) -> dict[str, Any]:
    resolved_assumptions = [item.strip() for item in assumptions if item and item.strip()]
    if not resolved_assumptions:
        raise ValueError("Scenario assumptions are required.")
    config = _config(manifest)
    if situation_id:
        situation = fetch_situation_model(config, situation_id=situation_id)
    else:
        situation = fetch_latest_situation_model(config, domain=domain_id)
    if situation is None:
        situation = materialize_situation_model(manifest, domain_id=domain_id)
    payload = {
        "scenario_id": hashlib.sha256(f"{situation['situation_id']}:{scenario_name}:{'|'.join(resolved_assumptions)}".encode("utf-8")).hexdigest()[:24],
        "situation_id": situation["situation_id"],
        "scenario_name": scenario_name,
        "status": "active",
        "confidence": confidence,
        "review_due_at": review_due_at,
        "summary_text": (expected_outcomes or [scenario_name])[0],
        "assumptions": resolved_assumptions,
        "changed_variables": changed_variables or {},
        "expected_outcomes": expected_outcomes or [],
        "failure_modes": failure_modes or [],
    }
    return store_scenario_run(config, payload)


def review_scenario(
    manifest: dict[str, Any],
    *,
    scenario_id: str,
    status: str,
    review_summary: str,
    outcome_event_id: str | None = None,
) -> dict[str, Any]:
    config = _config(manifest)
    scenarios = fetch_scenario_runs(config, limit=200)
    scenario = next((item for item in scenarios if item["scenario_id"] == scenario_id or item["id"] == scenario_id), None)
    if scenario is None:
        raise ValueError(f"Unknown scenario_id: {scenario_id}")
    if outcome_event_id is not None and fetch_event(config, event_id=outcome_event_id) is None:
        raise ValueError(f"Unknown outcome_event_id: {outcome_event_id}")
    review = {
        "review_id": hashlib.sha256(f"{scenario_id}:{status}:{review_summary}".encode("utf-8")).hexdigest()[:24],
        "scenario_id": scenario["scenario_id"],
        "outcome_event_id": outcome_event_id,
        "status": status,
        "review_summary": review_summary,
    }
    return store_forecast_review(config, review)


def reconstruct_timeline(
    manifest: dict[str, Any],
    *,
    timestamp: datetime,
    domain: str | None = None,
    window_hours: int = 6,
    limit: int = 3,
) -> dict[str, Any]:
    config = _config(manifest)
    payload = timeline_state(
        config,
        target=timestamp,
        domain=domain,
        window_hours=window_hours,
        limit=limit,
        visibility="raw",
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
    normalized_entry = _normalize_record_entry(entry)
    skip_generic_source_archives = bool(entry.get("skip_generic_source_archives"))
    resolved_mem0_status = default_mem0_status(normalized_entry, source_kind=source_kind)
    if resolved_mem0_status is not None:
        normalized_entry["mem0_status"] = resolved_mem0_status
    config = _config(manifest)

    if source_kind == "chronicle_mcp":
        memory_guard = evaluate_memory_guard(config, normalized_entry, source_kind=source_kind)
        normalized_entry["memory_guard"] = memory_guard
        if memory_guard["verdict"] == "local_only":
            normalized_entry["mem0_status"] = "off"

    if dedupe:
        with open_connection(config) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = _find_recent_exact_duplicate(
                config,
                normalized_entry,
                window_hours=dedupe_window_hours,
                connection=connection,
            )
            if existing is not None:
                existing["chronicle_status"] = "existing"
                existing["chronicle_db_path"] = str(config.db_path)
                existing["chronicle_error"] = None
                existing["dedupe_status"] = "exact_duplicate"
                existing["dedupe_window_hours"] = dedupe_window_hours
                existing["artifacts_written"] = 0
                return existing
            stored = store_event(
                config,
                normalized_entry,
                source_kind=source_kind,
                imported_from=imported_from,
                connection=connection,
            )
    else:
        stored = store_event(config, normalized_entry, source_kind=source_kind, imported_from=imported_from)
    stored["chronicle_status"] = "stored"
    stored["chronicle_db_path"] = str(config.db_path)
    stored["chronicle_error"] = None

    artifacts_written = 0
    if not skip_generic_source_archives:
        for source_file in entry.get("source_files") or []:
            path = Path(source_file).expanduser()
            artifact = store_artifact_from_path(
                config,
                source_path=path,
                artifact_type="event-source",
                observed_at_utc=stored["recorded_at"],
                entity_id=stored.get("entity_id"),
                metadata={"event_id": stored["id"], "category": stored.get("category")},
            )
            if artifact is None:
                continue
            link_artifact(
                config,
                artifact_id=artifact["id"],
                target_type="event",
                target_id=stored["id"],
                link_role="source",
                metadata={"path": str(path)},
            )
            artifacts_written += 1

    if append_compat:
        ledger_row = dict(stored)
        ledger_row["source_files"] = entry.get("source_files") or []
        ledger_row["mem0_status"] = entry.get("mem0_status")
        ledger_row["mem0_error"] = entry.get("mem0_error")
        ledger_row["mem0_raw"] = entry.get("mem0_raw")
        append_jsonl(_compat_path(manifest, "ledger_file"), ledger_row)

    stored["artifacts_written"] = artifacts_written
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

    openclaw = snapshot.get("openclaw", {})
    add(openclaw.get("state_path"), "openclaw-state", "system", "openclaw")
    add(openclaw.get("brief_path"), "openclaw-brief", "system", "openclaw")
    add(openclaw.get("leads_path"), "openclaw-leads", "system", "openclaw")
    add(openclaw.get("daily_log_path"), "openclaw-daily-log", "system", "openclaw")

    digest = snapshot.get("digest", {})
    add(digest.get("status_path"), "digest-status", "system", "digest")
    add(digest.get("synthesis_path"), "digest-synthesis", "system", "digest")
    add(digest.get("previous_summary_path"), "digest-previous-summary", "system", "digest")

    portfolio = snapshot.get("portfolio_assets", {})
    add(portfolio.get("path"), "portfolio-asset-manifest", "project", "portfolio")
    add(manifest["paths"].get("portfolio_asset_manifest"), "portfolio-asset-manifest", "project", "portfolio")

    return specs


def _refresh_relations(config, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    relations: list[dict[str, Any]] = []
    portfolio = snapshot.get("portfolio_assets", {})
    missing_assets_total = portfolio.get("missing_assets_total")
    rationale = (
        f"Snapshot {snapshot.get('id')} captured portfolio state with missing_assets_total={missing_assets_total}."
    )

    relation_specs = [
        ("system", "openclaw", "supports", "project", "job-search", "OpenClaw is the autonomous execution layer for job search."),
        ("system", "digest", "informs", "project", "job-search", "Digest supplies world context that informs job search decisions."),
        ("project", "portfolio", "gates", "project", "linkedin", rationale),
        ("project", "linkedin", "gates", "project", "job-search", "LinkedIn follow-up depends on portfolio readiness."),
    ]

    for from_type, from_name, relation_type, to_type, to_name, why in relation_specs:
        _validate_relation(relation_type)
        relations.append(
            upsert_relation(
                config,
                from_entity_type=from_type,
                from_entity_name=from_name,
                relation_type=relation_type,
                to_entity_type=to_type,
                to_entity_name=to_name,
                rationale=why,
                metadata={"snapshot_id": snapshot.get("id")},
            )
        )
    return relations


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

    if append_compat:
        append_jsonl(_compat_path(manifest, "snapshot_file"), stored)

    run_id = start_ingest_run(
        config,
        adapter="runtime_snapshot",
        source_ref=stored["id"],
        metadata={"domain": stored.get("domain")},
    )
    artifact_count = 0
    relation_count = 0
    normalized_entities: list[dict[str, Any]] = []
    situation_model: dict[str, Any] | None = None
    lens_runs: list[dict[str, Any]] = []
    try:
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

        relation_count = len(_refresh_relations(config, stored))
        normalized_entities = materialize_normalized_entities(
            manifest,
            domain_id=stored.get("domain") or "global",
            snapshot=stored,
            event_limit=64,
        )
        situation_model = materialize_situation_model(
            manifest,
            domain_id=stored.get("domain") or "global",
            snapshot=stored,
            event_limit=24,
            normalized_entities=normalized_entities,
        )
        lens_runs = run_lenses(
            manifest,
            domain_id=stored.get("domain") or "global",
            situation_model=situation_model,
            persist=True,
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
                "situation_id": situation_model.get("situation_id") if situation_model else None,
                "lens_run_count": len(lens_runs),
            },
        )
    except Exception as exc:
        finish_ingest_run(
            config,
            run_id=run_id,
            status="failed",
            items_seen=len(_snapshot_artifact_specs(manifest, stored)),
            items_written=artifact_count,
            error_text=str(exc),
        )
        raise

    projections = []
    if render_generated:
        preferred_snapshot = stored if stored.get("domain") == "global" else None
        projections = render_projections(manifest, snapshot=preferred_snapshot)

    stored["artifacts_written"] = artifact_count
    stored["relations_written"] = relation_count
    stored["normalized_entities"] = normalized_entities
    stored["situation_model"] = situation_model
    stored["lens_runs"] = lens_runs
    stored["projection_runs"] = projections
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
    attach_bundle = build_attach_bundle(
        manifest,
        domain_id=domain_id,
        agent=agent,
        title=title,
        focus=focus,
        capture=capture,
    )
    snapshot = attach_bundle["snapshot"]
    prompt = render_activation_prompt(snapshot)
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
    projected_summary = dict(summary)
    projected_summary["projection_runs"] = int(summary.get("projection_runs", 0)) + 2

    global_events = fetch_recent_events(config, limit=8)
    job_events = fetch_recent_events(config, limit=8, domain="job_search")
    if not job_events:
        job_events = fetch_project_events(config, project="job-search", limit=8)

    outputs: list[dict[str, Any]] = []

    job_path = _source_path(manifest, "job_search")
    job_content = render_job_search_status(snapshot, job_events)
    job_path.write_text(job_content, encoding="utf-8")
    outputs.append(
        {
            "projection_name": "job-search-status",
            "target_path": str(job_path),
            "content_sha256": _sha256_text(job_content),
        }
    )

    status_path = _source_path(manifest, "status")
    block = render_status_generated_block(snapshot, projected_summary, global_events)
    status_content = update_status_file(status_path, block)
    outputs.append(
        {
            "projection_name": "status-generated-view",
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
    situation = fetch_latest_situation_model(config, domain=domain)
    lens_runs = fetch_lens_runs(config, situation_id=situation["situation_id"]) if situation else []
    return {
        "contract_name": ACTIVATION_CONTRACT_NAME,
        "contract_version": ACTIVATION_CONTRACT_VERSION,
        "domain": domain,
        "summary": summary,
        "latest_snapshot": snapshot,
        "recent_events": events,
        "freshness_audit": build_freshness_audit(manifest, domain_id=domain),
        "source_audit": build_sources_audit(manifest, domain_id=domain),
        "latest_situation_model": situation,
        "latest_lens_runs": lens_runs,
    }


def project_state(
    manifest: dict[str, Any],
    *,
    project: str,
    event_limit: int = 10,
) -> dict[str, Any]:
    config = _config(manifest)
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
    interpretation_hits: list[dict[str, Any]] = []
    scenario_hits: list[dict[str, Any]] = []
    forecast_review_hits: list[dict[str, Any]] = []

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

        situation = fetch_latest_situation_model(config, domain=domain or "global")
        if situation is not None:
            interpretation_hits.extend(
                _search_derived_hits(
                    query,
                    [situation],
                    record_kind="situation_model",
                    text_builder=lambda item: json.dumps(item, ensure_ascii=False),
                    limit=1,
                )
            )
            lens_runs = fetch_lens_runs(config, situation_id=situation["situation_id"], limit=20)
            interpretation_hits.extend(
                _search_derived_hits(
                    query,
                    lens_runs,
                    record_kind="lens_run",
                    text_builder=lambda item: " ".join(
                        [
                            item.get("lens") or "",
                            item.get("summary_text") or "",
                            json.dumps(item.get("findings") or [], ensure_ascii=False),
                        ]
                    ),
                    limit=limit,
                )
            )

            if mode == "truth_plus_interpretation_plus_scenarios":
                scenarios = fetch_scenario_runs(config, situation_id=situation["situation_id"], limit=20)
                scenario_hits = _search_derived_hits(
                    query,
                    scenarios,
                    record_kind="scenario_run",
                    text_builder=lambda item: json.dumps(item, ensure_ascii=False),
                    limit=limit,
                )
                reviews = fetch_forecast_reviews(config, limit=20)
                forecast_review_hits = _search_derived_hits(
                    query,
                    reviews,
                    record_kind="forecast_review",
                    text_builder=lambda item: json.dumps(item, ensure_ascii=False),
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
        "status_hits": status_hits,
        "normalized_entity_hits": normalized_entity_hits,
        "interpretation_hits": interpretation_hits[:limit],
        "scenario_hits": scenario_hits[:limit] if mode == "truth_plus_interpretation_plus_scenarios" else [],
        "forecast_review_hits": forecast_review_hits[:limit] if mode == "truth_plus_interpretation_plus_scenarios" else [],
        "briefing_hits": [],
        "mem0_dump_hits": mem0_hits,
    }
