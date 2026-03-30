from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import mimetypes
import re
import shutil
import sqlite3
import uuid
from typing import Any
from zoneinfo import ZoneInfo

from .config import ChronicleConfig, default_config, ensure_runtime_dirs
from .db import apply_migrations, connect, utc_now


POINTER_ONLY_STORAGE_PREFIX = "pointer://"


def config_from_manifest(manifest: dict[str, Any]) -> ChronicleConfig:
    paths = manifest.get("paths", {})
    artifact_settings = manifest.get("artifacts", {})
    db_value = paths.get("chronicle_db")
    db_path = Path(db_value).expanduser() if db_value else None
    config = default_config(db_path)
    timezone_name = manifest.get("settings", {}).get("timezone") or config.timezone
    artifact_dir = paths.get("chronicle_artifact_dir")
    configured_extensions = artifact_settings.get("allowed_extensions") or config.artifact_allowed_extensions
    normalized_extensions = tuple(
        sorted(
            {
                str(item).strip().casefold().lstrip(".")
                for item in configured_extensions
                if str(item).strip()
            }
        )
    ) or config.artifact_allowed_extensions
    max_copy_size_mb = artifact_settings.get("max_copy_size_mb")
    artifact_max_copy_bytes = config.artifact_max_copy_bytes
    if max_copy_size_mb is not None:
        artifact_max_copy_bytes = max(0, int(float(max_copy_size_mb) * 1024 * 1024))

    if (
        timezone_name != config.timezone
        or artifact_dir
        or artifact_max_copy_bytes != config.artifact_max_copy_bytes
        or normalized_extensions != config.artifact_allowed_extensions
        or artifact_settings.get("pointer_only_enabled") is not None
        or artifact_settings.get("pointer_only_disallowed") is not None
        or artifact_settings.get("follow_symlinks") is not None
    ):
        config = replace(
            config,
            timezone=timezone_name,
            artifact_dir=Path(artifact_dir).expanduser() if artifact_dir else config.artifact_dir,
            artifact_max_copy_bytes=artifact_max_copy_bytes,
            artifact_allowed_extensions=normalized_extensions,
            artifact_pointer_only_enabled=bool(
                artifact_settings.get(
                    "pointer_only_enabled",
                    artifact_settings.get("pointer_only_disallowed", config.artifact_pointer_only_enabled),
                )
            ),
            artifact_follow_symlinks=bool(
                artifact_settings.get("follow_symlinks", config.artifact_follow_symlinks)
            ),
        )
    return config


def open_connection(config: ChronicleConfig) -> sqlite3.Connection:
    ensure_runtime_dirs(config)
    connection = connect(config.db_path)
    apply_migrations(connection, config)
    return connection


def chronicle_db_exists(config: ChronicleConfig) -> bool:
    return config.db_path.exists()


def _json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _load_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _parse_iso(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_local_iso(timestamp: str, timezone_name: str) -> str:
    local_tz = ZoneInfo(timezone_name)
    return _parse_iso(timestamp).astimezone(local_tz).isoformat(timespec="seconds")


def parse_when(value: str, timezone_name: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(timezone_name))
    return dt.astimezone(timezone.utc)


def _slugify(value: str) -> str:
    slug = value.strip().casefold()
    cleaned = []
    for char in slug:
        if char.isalnum():
            cleaned.append(char)
            continue
        if char in {" ", "_", "-", "/"}:
            cleaned.append("-")
    normalized = "".join(cleaned).strip("-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or "unknown"


def _entity_name(slug: str) -> str:
    return slug.replace("-", " ").title()


def _project_from_entity_id(entity_id: str | None) -> str | None:
    if not entity_id or not entity_id.startswith("project:"):
        return None
    return entity_id.split(":", 1)[1]


def _fts_query(query: str) -> str:
    tokens = [
        token.replace('"', '""')
        for token in re.findall(r"[A-Za-zА-Яа-я0-9$+._-]+", query.casefold())
        if len(token) >= 2
    ]
    if not tokens:
        return '""'
    return " AND ".join(f'"{token}"' for token in tokens)


def _ensure_entity(
    connection: sqlite3.Connection,
    entity_type: str,
    slug_or_name: str | None,
    recorded_at_utc: str,
    *,
    name: str | None = None,
    status: str = "active",
    summary: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    if not slug_or_name:
        return None, None

    slug = _slugify(slug_or_name)
    entity_id = f"{entity_type}:{slug}"
    connection.execute(
        """
        INSERT INTO entities(
            id,
            entity_type,
            slug,
            name,
            status,
            summary,
            tags_json,
            metadata_json,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            status = excluded.status,
            summary = COALESCE(excluded.summary, entities.summary),
            tags_json = COALESCE(excluded.tags_json, entities.tags_json),
            metadata_json = COALESCE(excluded.metadata_json, entities.metadata_json),
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            entity_id,
            entity_type,
            slug,
            name or _entity_name(slug),
            status,
            summary,
            _json(tags or [slug]),
            _json(metadata or {"source": slug_or_name}),
            recorded_at_utc,
            recorded_at_utc,
        ),
    )
    return entity_id, entity_type


def ensure_entity(
    config: ChronicleConfig,
    *,
    entity_type: str,
    slug_or_name: str,
    recorded_at_utc: str | None = None,
    name: str | None = None,
    status: str = "active",
    summary: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str, str]:
    resolved_at = recorded_at_utc or utc_now()
    with open_connection(config) as connection, connection:
        entity_id, resolved_type = _ensure_entity(
            connection,
            entity_type,
            slug_or_name,
            resolved_at,
            name=name,
            status=status,
            summary=summary,
            tags=tags,
            metadata=metadata,
        )
    if not entity_id or not resolved_type:
        raise ValueError("Failed to upsert entity")
    return entity_id, resolved_type


def _normalized_entity_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "entity_type": row["entity_type"],
        "canonical_key": row["canonical_key"],
        "canonical_name": row["canonical_name"],
        "aliases": json.loads(row["aliases_json"]),
        "source_refs": json.loads(row["source_refs_json"]),
        "status": row["status"],
        "metadata": _load_json(row["metadata_json"]),
        "created_at_utc": row["created_at_utc"],
        "updated_at_utc": row["updated_at_utc"],
    }


def upsert_normalized_entity(
    config: ChronicleConfig,
    *,
    entity_type: str,
    canonical_key: str,
    canonical_name: str,
    aliases: list[str],
    source_refs: list[dict[str, Any]],
    status: str = "active",
    metadata: dict[str, Any] | None = None,
    replace_existing: bool = False,
) -> dict[str, Any]:
    resolved_aliases = sorted({item.strip() for item in aliases if item and item.strip()}, key=str.casefold)
    if not resolved_aliases:
        resolved_aliases = [canonical_name]
    entity_id = f"{entity_type}:{canonical_key}"
    now = utc_now()
    with open_connection(config) as connection, connection:
        existing = connection.execute(
            """
            SELECT aliases_json, source_refs_json, metadata_json
            FROM normalized_entities
            WHERE entity_type = ? AND canonical_key = ?
            LIMIT 1
            """,
            (entity_type, canonical_key),
        ).fetchone()
        if existing:
            merged_metadata = _load_json(existing["metadata_json"])
            if metadata:
                merged_metadata.update(metadata)
            metadata = merged_metadata
            if not replace_existing:
                resolved_aliases = sorted(
                    {
                        *resolved_aliases,
                        *json.loads(existing["aliases_json"]),
                    },
                    key=str.casefold,
                )
                existing_refs = json.loads(existing["source_refs_json"])
                source_refs = existing_refs + [item for item in source_refs if item not in existing_refs]
        connection.execute(
            """
            INSERT INTO normalized_entities(
                id,
                entity_type,
                canonical_key,
                canonical_name,
                aliases_json,
                source_refs_json,
                status,
                metadata_json,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, canonical_key) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                aliases_json = excluded.aliases_json,
                source_refs_json = excluded.source_refs_json,
                status = excluded.status,
                metadata_json = COALESCE(excluded.metadata_json, normalized_entities.metadata_json),
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                entity_id,
                entity_type,
                canonical_key,
                canonical_name,
                _json(resolved_aliases),
                _json(source_refs),
                status,
                _json(metadata) if metadata else None,
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM normalized_entities
            WHERE entity_type = ? AND canonical_key = ?
            LIMIT 1
            """,
            (entity_type, canonical_key),
        ).fetchone()
    return _normalized_entity_row_to_entry(row)


def fetch_normalized_entities(
    config: ChronicleConfig,
    *,
    entity_type: str | None = None,
    status: str | None = "active",
    limit: int = 200,
) -> list[dict[str, Any]]:
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM normalized_entities
            WHERE (? IS NULL OR entity_type = ?)
              AND (? IS NULL OR status = ?)
            ORDER BY updated_at_utc DESC, canonical_name ASC
            LIMIT ?
            """,
            (entity_type, entity_type, status, status, limit),
        ).fetchall()
    return [_normalized_entity_row_to_entry(row) for row in rows]


def mark_missing_normalized_entities_inactive(
    config: ChronicleConfig,
    *,
    active_entity_ids: list[str],
) -> int:
    now = utc_now()
    with open_connection(config) as connection, connection:
        if active_entity_ids:
            placeholders = ", ".join("?" for _ in active_entity_ids)
            cursor = connection.execute(
                f"""
                UPDATE normalized_entities
                SET status = 'inactive',
                    updated_at_utc = ?
                WHERE status != 'inactive'
                  AND id NOT IN ({placeholders})
                """,
                (now, *active_entity_ids),
            )
        else:
            cursor = connection.execute(
                """
                UPDATE normalized_entities
                SET status = 'inactive',
                    updated_at_utc = ?
                WHERE status != 'inactive'
                """,
                (now,),
            )
    return int(cursor.rowcount or 0)


def _situation_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    payload = _load_json(row["payload_json"])
    payload.setdefault("situation_id", row["id"])
    payload.setdefault("id", row["id"])
    payload.setdefault("domain", row["domain"])
    payload.setdefault("snapshot_id", row["snapshot_id"])
    payload.setdefault("valid_at", row["valid_at_utc"])
    payload.setdefault("status", row["status"])
    payload.setdefault("summary_text", row["summary_text"])
    payload["derived_from"] = json.loads(row["derived_from_json"])
    payload["created_at_utc"] = row["created_at_utc"]
    payload["updated_at_utc"] = row["updated_at_utc"]
    return payload


def store_situation_model(
    config: ChronicleConfig,
    situation_model: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(situation_model)
    situation_id = payload.get("situation_id") or payload.get("id") or str(uuid.uuid4())
    now = utc_now()
    valid_at = payload.get("valid_at") or payload.get("valid_at_utc") or now
    derived_from = payload.get("derived_from") or {}
    summary_text = payload.get("summary_text")
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO situation_models(
                id,
                domain,
                snapshot_id,
                valid_at_utc,
                status,
                summary_text,
                payload_json,
                derived_from_json,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                domain = excluded.domain,
                snapshot_id = excluded.snapshot_id,
                valid_at_utc = excluded.valid_at_utc,
                status = excluded.status,
                summary_text = excluded.summary_text,
                payload_json = excluded.payload_json,
                derived_from_json = excluded.derived_from_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                situation_id,
                payload.get("domain") or "global",
                payload.get("snapshot_id"),
                valid_at,
                payload.get("status") or "active",
                summary_text,
                _json(payload),
                _json(derived_from),
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM situation_models
            WHERE id = ?
            LIMIT 1
            """,
            (situation_id,),
        ).fetchone()
    return _situation_row_to_entry(row)


def fetch_latest_situation_model(
    config: ChronicleConfig,
    *,
    domain: str = "global",
    snapshot_id: str | None = None,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM situation_models
            WHERE domain = ?
              AND (? IS NULL OR snapshot_id = ?)
            ORDER BY valid_at_utc DESC, updated_at_utc DESC
            LIMIT 1
            """,
            (domain, snapshot_id, snapshot_id),
        ).fetchone()
    if not row:
        return None
    return _situation_row_to_entry(row)


def fetch_situation_model(
    config: ChronicleConfig,
    *,
    situation_id: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM situation_models
            WHERE id = ?
            LIMIT 1
            """,
            (situation_id,),
        ).fetchone()
    if not row:
        return None
    return _situation_row_to_entry(row)


def _lens_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "run_id": row["id"],
        "situation_id": row["situation_id"],
        "lens": row["lens"],
        "status": row["status"],
        "confidence": float(row["confidence"]),
        "summary_text": row["summary_text"],
        "findings": json.loads(row["findings_json"]),
        "evidence_refs": json.loads(row["evidence_refs_json"]),
        "created_at_utc": row["created_at_utc"],
        "updated_at_utc": row["updated_at_utc"],
    }


def store_lens_run(
    config: ChronicleConfig,
    lens_run: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(lens_run)
    run_id = payload.get("run_id") or payload.get("id") or str(uuid.uuid4())
    now = utc_now()
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO lens_runs(
                id,
                situation_id,
                lens,
                status,
                confidence,
                summary_text,
                findings_json,
                evidence_refs_json,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(situation_id, lens) DO UPDATE SET
                id = excluded.id,
                status = excluded.status,
                confidence = excluded.confidence,
                summary_text = excluded.summary_text,
                findings_json = excluded.findings_json,
                evidence_refs_json = excluded.evidence_refs_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                run_id,
                payload["situation_id"],
                payload["lens"],
                payload.get("status") or "completed",
                float(payload.get("confidence", 0.0)),
                payload.get("summary_text"),
                _json(payload.get("findings") or []),
                _json(payload.get("evidence_refs") or []),
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM lens_runs
            WHERE situation_id = ? AND lens = ?
            LIMIT 1
            """,
            (payload["situation_id"], payload["lens"]),
        ).fetchone()
    return _lens_row_to_entry(row)


def fetch_lens_runs(
    config: ChronicleConfig,
    *,
    situation_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM lens_runs
            WHERE situation_id = ?
            ORDER BY updated_at_utc DESC, lens ASC
            LIMIT ?
            """,
            (situation_id, limit),
        ).fetchall()
    return [_lens_row_to_entry(row) for row in rows]


def _scenario_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    payload = _load_json(row["payload_json"])
    payload.setdefault("scenario_id", row["id"])
    payload.setdefault("id", row["id"])
    payload.setdefault("situation_id", row["situation_id"])
    payload.setdefault("scenario_name", row["scenario_name"])
    payload.setdefault("status", row["status"])
    payload.setdefault("confidence", float(row["confidence"]))
    payload.setdefault("review_due_at", row["review_due_at_utc"])
    payload.setdefault("summary_text", row["summary_text"])
    payload["assumptions"] = json.loads(row["assumptions_json"])
    payload["changed_variables"] = json.loads(row["changed_variables_json"])
    payload["expected_outcomes"] = json.loads(row["expected_outcomes_json"])
    payload["failure_modes"] = json.loads(row["failure_modes_json"])
    payload["created_at_utc"] = row["created_at_utc"]
    payload["updated_at_utc"] = row["updated_at_utc"]
    return payload


def store_scenario_run(
    config: ChronicleConfig,
    scenario_run: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(scenario_run)
    run_id = payload.get("scenario_id") or payload.get("id") or str(uuid.uuid4())
    now = utc_now()
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO scenario_runs(
                id,
                situation_id,
                scenario_name,
                status,
                confidence,
                review_due_at_utc,
                summary_text,
                assumptions_json,
                changed_variables_json,
                expected_outcomes_json,
                failure_modes_json,
                payload_json,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                confidence = excluded.confidence,
                review_due_at_utc = excluded.review_due_at_utc,
                summary_text = excluded.summary_text,
                assumptions_json = excluded.assumptions_json,
                changed_variables_json = excluded.changed_variables_json,
                expected_outcomes_json = excluded.expected_outcomes_json,
                failure_modes_json = excluded.failure_modes_json,
                payload_json = excluded.payload_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                run_id,
                payload["situation_id"],
                payload["scenario_name"],
                payload.get("status") or "active",
                float(payload.get("confidence", 0.0)),
                payload.get("review_due_at"),
                payload.get("summary_text"),
                _json(payload.get("assumptions") or []),
                _json(payload.get("changed_variables") or {}),
                _json(payload.get("expected_outcomes") or []),
                _json(payload.get("failure_modes") or []),
                _json(payload),
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM scenario_runs
            WHERE id = ?
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
    return _scenario_row_to_entry(row)


def fetch_scenario_runs(
    config: ChronicleConfig,
    *,
    situation_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM scenario_runs
            WHERE (? IS NULL OR situation_id = ?)
            ORDER BY updated_at_utc DESC, created_at_utc DESC
            LIMIT ?
            """,
            (situation_id, situation_id, limit),
        ).fetchall()
    return [_scenario_row_to_entry(row) for row in rows]


def _forecast_review_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    payload = _load_json(row["payload_json"])
    payload.setdefault("review_id", row["id"])
    payload.setdefault("id", row["id"])
    payload.setdefault("scenario_id", row["scenario_id"])
    payload.setdefault("outcome_event_id", row["outcome_event_id"])
    payload.setdefault("status", row["status"])
    payload.setdefault("review_summary", row["review_summary"])
    payload["created_at_utc"] = row["created_at_utc"]
    payload["updated_at_utc"] = row["updated_at_utc"]
    return payload


def store_forecast_review(
    config: ChronicleConfig,
    review: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(review)
    review_id = payload.get("review_id") or payload.get("id") or str(uuid.uuid4())
    now = utc_now()
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO forecast_reviews(
                id,
                scenario_id,
                outcome_event_id,
                status,
                review_summary,
                payload_json,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                outcome_event_id = excluded.outcome_event_id,
                status = excluded.status,
                review_summary = excluded.review_summary,
                payload_json = excluded.payload_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                review_id,
                payload["scenario_id"],
                payload.get("outcome_event_id"),
                payload["status"],
                payload.get("review_summary"),
                _json(payload),
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM forecast_reviews
            WHERE id = ?
            LIMIT 1
            """,
            (review_id,),
        ).fetchone()
    return _forecast_review_row_to_entry(row)


def fetch_forecast_reviews(
    config: ChronicleConfig,
    *,
    scenario_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM forecast_reviews
            WHERE (? IS NULL OR scenario_id = ?)
            ORDER BY updated_at_utc DESC, created_at_utc DESC
            LIMIT ?
            """,
            (scenario_id, scenario_id, limit),
        ).fetchall()
    return [_forecast_review_row_to_entry(row) for row in rows]


def fetch_event(
    config: ChronicleConfig,
    *,
    event_id: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE e.id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()
    if not row:
        return None
    return _event_row_to_entry(row)


def _ensure_project_entity(
    connection: sqlite3.Connection,
    project: str | None,
    recorded_at_utc: str,
) -> tuple[str | None, str | None]:
    return _ensure_entity(
        connection,
        "project",
        project,
        recorded_at_utc,
        metadata={"project_key": project} if project else None,
    )


def snapshot_summary(snapshot: dict[str, Any]) -> str:
    lines: list[str] = []
    label = snapshot.get("title") or snapshot.get("label")
    if label:
        lines.append(f"title={label}")

    domain = snapshot.get("domain")
    if domain:
        lines.append(f"domain={domain}")

    portfolio = snapshot.get("portfolio_assets") or {}
    if portfolio:
        lines.append(
            "portfolio_missing="
            f"{portfolio.get('missing_assets_total', 'unknown')}"
        )
        lines.append(
            "portfolio_og_needed="
            f"{portfolio.get('og_images_needed', 'unknown')}"
        )

    openclaw = snapshot.get("openclaw") or {}
    if openclaw:
        lines.append(
            "openclaw_jobs_found_today="
            f"{openclaw.get('jobs_found_today', 'unknown')}"
        )
        lines.append(
            "openclaw_applications_sent_today="
            f"{openclaw.get('applications_sent_today', 'unknown')}"
        )

    digest = snapshot.get("digest") or {}
    status = digest.get("status") or {}
    if isinstance(status, dict) and status:
        lines.append(
            "digest_status="
            f"{status.get('status', 'unknown')}"
        )
    for headline in (digest.get("world_headlines") or [])[:2]:
        lines.append(headline)

    if snapshot.get("focus"):
        lines.append(f"focus={snapshot['focus']}")

    return " | ".join(lines)


def _outbox_status(mem0_status: str | None) -> str:
    if mem0_status in {"stored", "synced"}:
        return "synced"
    if mem0_status == "failed":
        return "failed"
    if mem0_status in {"off", "skipped"}:
        return "skipped"
    return "pending"


def _attempt_count(mem0_status: str | None) -> int:
    if mem0_status in {"stored", "failed"}:
        return 1
    return 0


def _event_mem0_status(mem0_status: str | None) -> str | None:
    if mem0_status in {"stored", "synced"}:
        return "stored"
    if mem0_status == "pending":
        return "queued"
    if mem0_status in {"failed", "queued"}:
        return mem0_status
    if mem0_status in {"off", "skipped"}:
        return "skipped"
    return mem0_status


def _normalize_event_visibility(visibility: str | None) -> str:
    normalized = str(visibility or "default").strip().casefold()
    if normalized not in {"default", "raw"}:
        raise ValueError(f"Unsupported visibility: {visibility}")
    return normalized


def _event_payload(entry: dict[str, Any]) -> dict[str, Any]:
    payload = dict(entry)
    payload.setdefault("recorded_at", utc_now())
    return payload


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bytes_sha256(content: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()


def _artifact_extension(path: Path) -> str:
    return path.suffix.casefold().lstrip(".")


def _pointer_storage_path(artifact_type: str, sha256: str, source_name: str) -> str:
    return f"{POINTER_ONLY_STORAGE_PREFIX}{artifact_type}/{sha256}/{source_name}"


def _artifact_decision(
    config: ChronicleConfig,
    *,
    source_path: Path,
    source_size: int,
) -> tuple[str, str | None]:
    if source_path.is_symlink() and not config.artifact_follow_symlinks:
        return "pointer_only" if config.artifact_pointer_only_enabled else "skip", "symlink_rejected"
    if source_size > config.artifact_max_copy_bytes:
        return "pointer_only" if config.artifact_pointer_only_enabled else "skip", "size_limit_exceeded"
    if _artifact_extension(source_path) not in config.artifact_allowed_extensions:
        return "pointer_only" if config.artifact_pointer_only_enabled else "skip", "extension_not_allowed"
    return "copy", None


def _store_artifact_record(
    connection: sqlite3.Connection,
    *,
    artifact_id: str,
    artifact_type: str,
    sha256: str,
    storage_path: str,
    source_path: str | None,
    mime_type: str | None,
    observed_at: str,
    resolved_entity_id: str | None,
    artifact_metadata: dict[str, Any],
) -> sqlite3.Row:
    return connection.execute(
        """
        INSERT INTO artifacts(
            id,
            artifact_type,
            sha256,
            storage_path,
            source_path,
            mime_type,
            observed_at_utc,
            entity_id,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256, storage_path) DO UPDATE SET
            observed_at_utc = excluded.observed_at_utc,
            entity_id = COALESCE(excluded.entity_id, artifacts.entity_id),
            metadata_json = COALESCE(excluded.metadata_json, artifacts.metadata_json)
        RETURNING id, artifact_type, sha256, storage_path, source_path, entity_id
        """,
        (
            artifact_id,
            artifact_type,
            sha256,
            storage_path,
            source_path,
            mime_type,
            observed_at,
            resolved_entity_id,
            _json(artifact_metadata),
        ),
    ).fetchone()


def _upsert_mem0_outbox(
    connection: sqlite3.Connection,
    event_id: str,
    payload: dict[str, Any],
    created_at_utc: str,
    mem0_status: str | None,
    mem0_error: str | None,
    *,
    attempts: int | None = None,
    last_attempt_at_utc: str | None = None,
    synced_at_utc: str | None = None,
) -> None:
    sync_status = _outbox_status(mem0_status)
    if attempts is None:
        attempts = _attempt_count(mem0_status)
    if last_attempt_at_utc is None and attempts:
        last_attempt_at_utc = created_at_utc
    if synced_at_utc is None and sync_status == "synced":
        synced_at_utc = created_at_utc

    connection.execute(
        """
        INSERT INTO mem0_outbox(
            id,
            event_id,
            collection_name,
            operation,
            status,
            attempts,
            last_attempt_at_utc,
            last_error,
            payload_json,
            created_at_utc,
            synced_at_utc
        )
        VALUES (?, ?, 'napaarnik_personal', 'add', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, operation) DO UPDATE SET
            status = excluded.status,
            attempts = excluded.attempts,
            last_attempt_at_utc = excluded.last_attempt_at_utc,
            last_error = excluded.last_error,
            payload_json = excluded.payload_json,
            synced_at_utc = excluded.synced_at_utc
        """,
        (
            str(uuid.uuid4()),
            event_id,
            sync_status,
            attempts,
            last_attempt_at_utc,
            mem0_error,
            _json(payload),
            created_at_utc,
            synced_at_utc,
        ),
    )


def _event_domain_where_clause(alias: str = "e") -> str:
    return (
        f"(? IS NULL OR {alias}.circumstances = ? "
        f"OR ({alias}.circumstances IS NULL AND json_extract({alias}.payload_json, '$.domain') = ?))"
    )


def _event_project_where_clause(alias: str = "e") -> str:
    return (
        f"({alias}.entity_id = ? OR {alias}.title = ? "
        f"OR ({alias}.entity_id IS NULL AND {alias}.title IS NULL "
        f"AND json_extract({alias}.payload_json, '$.project') = ?))"
    )


def store_event(
    config: ChronicleConfig,
    entry: dict[str, Any],
    *,
    source_kind: str = "agent_command",
    imported_from: str = "chronicle.record",
    connection: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    payload = _event_payload(entry)
    event_id = payload.get("id") or str(uuid.uuid4())
    recorded_at_utc = payload.get("recorded_at") or utc_now()
    payload["mem0_status"] = _event_mem0_status(payload.get("mem0_status"))
    payload["id"] = event_id
    payload["recorded_at"] = recorded_at_utc

    entity_id, entity_type = None, None

    def _store(active_connection: sqlite3.Connection) -> None:
        nonlocal entity_id, entity_type
        entity_id, entity_type = _ensure_project_entity(
            active_connection,
            payload.get("project"),
            recorded_at_utc,
        )

        active_connection.execute(
            """
            INSERT INTO events(
                id,
                occurred_at_utc,
                occurred_at_local,
                timezone,
                recorded_at_utc,
                actor,
                event_type,
                category,
                entity_type,
                entity_id,
                title,
                text,
                why,
                circumstances,
                source_kind,
                source_path,
                source_hash,
                payload_json,
                imported_from,
                mem0_status,
                mem0_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                recorded_at_utc,
                to_local_iso(recorded_at_utc, config.timezone),
                config.timezone,
                recorded_at_utc,
                payload.get("agent"),
                payload.get("event_type") or f"agent.{payload.get('category') or 'note'}",
                payload.get("category"),
                entity_type,
                entity_id,
                payload.get("project"),
                payload.get("text") or "",
                payload.get("why"),
                payload.get("domain"),
                source_kind,
                (payload.get("source_files") or [None])[0],
                None,
                _json(payload),
                imported_from,
                payload.get("mem0_status"),
                payload.get("mem0_error"),
            ),
        )

        outbox_payload = {
            "text": payload.get("text") or "",
            "why": payload.get("why"),
            "category": payload.get("category"),
            "project": payload.get("project"),
        }
        if payload.get("memory_guard") is not None:
            outbox_payload["memory_guard"] = payload["memory_guard"]
        _upsert_mem0_outbox(
            active_connection,
            event_id,
            outbox_payload,
            recorded_at_utc,
            payload.get("mem0_status"),
            payload.get("mem0_error"),
        )

    if connection is not None:
        _store(connection)
    else:
        with open_connection(config) as connection, connection:
            _store(connection)

    payload["entity_id"] = entity_id
    payload["entity_type"] = entity_type
    return payload


def store_snapshot(
    config: ChronicleConfig,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(snapshot)
    payload.setdefault("id", str(uuid.uuid4()))
    payload.setdefault("captured_at_utc", utc_now())
    payload.setdefault("captured_at_local", to_local_iso(payload["captured_at_utc"], config.timezone))
    payload.setdefault("timezone", config.timezone)

    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO snapshots(
                id,
                captured_at_utc,
                captured_at_local,
                timezone,
                agent,
                domain,
                title,
                focus,
                label,
                summary_text,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                captured_at_utc = excluded.captured_at_utc,
                captured_at_local = excluded.captured_at_local,
                timezone = excluded.timezone,
                agent = excluded.agent,
                domain = excluded.domain,
                title = excluded.title,
                focus = excluded.focus,
                label = excluded.label,
                summary_text = excluded.summary_text,
                payload_json = excluded.payload_json
            """,
            (
                payload["id"],
                payload["captured_at_utc"],
                payload["captured_at_local"],
                payload["timezone"],
                payload.get("agent"),
                payload.get("domain"),
                payload.get("title"),
                payload.get("focus"),
                payload.get("label"),
                snapshot_summary(payload),
                _json(payload),
            ),
        )
    return payload


def _event_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    payload = _load_json(row["payload_json"])
    outbox_payload = _load_json(row["outbox_payload_json"]) if "outbox_payload_json" in row.keys() else {}
    outbox_status = row["outbox_status"] if "outbox_status" in row.keys() else None
    if outbox_status == "synced":
        mem0_status = "stored"
    elif outbox_status:
        mem0_status = outbox_status
    else:
        mem0_status = row["mem0_status"]

    if "outbox_status" in row.keys() and row["outbox_status"] is not None:
        mem0_error = row["outbox_last_error"] if row["outbox_last_error"] is not None else outbox_payload.get("mem0_error")
    else:
        mem0_error = row["mem0_error"]
    mem0_synced_at = (
        row["outbox_synced_at"]
        if "outbox_synced_at" in row.keys() and row["outbox_synced_at"] is not None
        else outbox_payload.get("mem0_synced_at", payload.get("mem0_synced_at"))
    )

    return {
        "id": row["id"],
        "recorded_at": row["occurred_at_utc"],
        "agent": payload.get("agent") or row["actor"],
        "branch": payload.get("branch"),
        "domain": payload.get("domain") or row["circumstances"],
        "category": row["category"],
        "project": payload.get("project") or _project_from_entity_id(row["entity_id"]),
        "text": row["text"],
        "why": row["why"],
        "source_files": payload.get("source_files") or [],
        "mem0_status": mem0_status,
        "mem0_error": mem0_error,
        "mem0_raw": outbox_payload.get("mem0_raw", payload.get("mem0_raw")),
        "mem0_synced_at": mem0_synced_at,
        "entity_id": row["entity_id"],
        "memory_guard": payload.get("memory_guard", outbox_payload.get("memory_guard")),
    }


def _snapshot_row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    payload = _load_json(row["payload_json"])
    payload.setdefault("id", row["id"])
    payload.setdefault("captured_at_utc", row["captured_at_utc"])
    return payload


def fetch_recent_events(
    config: ChronicleConfig,
    *,
    limit: int = 10,
    domain: str | None = None,
    visibility: str = "default",
    connection: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    resolved_visibility = _normalize_event_visibility(visibility)
    def _fetch(active_connection: sqlite3.Connection) -> list[sqlite3.Row]:
        return active_connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE (? = 'raw' OR COALESCE(json_extract(e.payload_json, '$.memory_guard.visibility'), '') != 'raw_only')
              AND """ + _event_domain_where_clause("e") + """
            ORDER BY e.occurred_at_utc DESC
            LIMIT ?
            """,
            (resolved_visibility, domain, domain, domain, limit),
        ).fetchall()

    if connection is not None:
        rows = _fetch(connection)
    else:
        with open_connection(config) as connection:
            rows = _fetch(connection)
    return [_event_row_to_entry(row) for row in rows]


def fetch_latest_snapshot(
    config: ChronicleConfig,
    *,
    domain: str | None = None,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT id, captured_at_utc, payload_json
            FROM snapshots
            WHERE (? IS NULL OR domain = ?)
            ORDER BY captured_at_utc DESC
            LIMIT 1
            """,
            (domain, domain),
        ).fetchone()
    if not row:
        return None
    return _snapshot_row_to_entry(row)


def fetch_project_events(
    config: ChronicleConfig,
    *,
    project: str,
    limit: int = 20,
    visibility: str = "default",
) -> list[dict[str, Any]]:
    resolved_visibility = _normalize_event_visibility(visibility)
    entity_id = f"project:{_slugify(project)}"
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE (? = 'raw' OR COALESCE(json_extract(e.payload_json, '$.memory_guard.visibility'), '') != 'raw_only')
              AND """ + _event_project_where_clause("e") + """
            ORDER BY e.occurred_at_utc DESC
            LIMIT ?
            """,
            (resolved_visibility, entity_id, project, project, limit),
        ).fetchall()
    return [_event_row_to_entry(row) for row in rows]


def search_events(
    config: ChronicleConfig,
    *,
    query: str,
    limit: int = 10,
    domain: str | None = None,
    visibility: str = "default",
) -> list[dict[str, Any]]:
    resolved_visibility = _normalize_event_visibility(visibility)
    fts_query = _fts_query(query)
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at,
                bm25(events_fts) AS rank
            FROM events_fts
            JOIN events AS e
                ON e.id = events_fts.event_id
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE (? = 'raw' OR COALESCE(json_extract(e.payload_json, '$.memory_guard.visibility'), '') != 'raw_only')
              AND events_fts MATCH ?
              AND """ + _event_domain_where_clause("e") + """
            ORDER BY rank
            LIMIT ?
            """,
            (resolved_visibility, fts_query, domain, domain, domain, limit),
        ).fetchall()
    return [_event_row_to_entry(row) for row in rows]


def fetch_events_between(
    config: ChronicleConfig,
    *,
    start_utc: str,
    end_utc: str,
    domain: str | None = None,
    limit: int = 500,
    visibility: str = "default",
) -> list[dict[str, Any]]:
    resolved_visibility = _normalize_event_visibility(visibility)
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE (? = 'raw' OR COALESCE(json_extract(e.payload_json, '$.memory_guard.visibility'), '') != 'raw_only')
              AND e.occurred_at_utc >= ?
              AND e.occurred_at_utc <= ?
              AND """ + _event_domain_where_clause("e") + """
            ORDER BY e.occurred_at_utc ASC
            LIMIT ?
            """,
            (resolved_visibility, start_utc, end_utc, domain, domain, domain, limit),
        ).fetchall()
    return [_event_row_to_entry(row) for row in rows]


def start_ingest_run(
    config: ChronicleConfig,
    *,
    adapter: str,
    source_ref: str | None = None,
    source_hash: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO ingest_runs(
                id,
                adapter,
                started_at_utc,
                status,
                source_ref,
                source_hash,
                metadata_json
            )
            VALUES (?, ?, ?, 'running', ?, ?, ?)
            """,
            (run_id, adapter, utc_now(), source_ref, source_hash, _json(metadata) if metadata else None),
        )
    return run_id


def finish_ingest_run(
    config: ChronicleConfig,
    *,
    run_id: str,
    status: str,
    items_seen: int,
    items_written: int,
    error_text: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at_utc = ?,
                status = ?,
                items_seen = ?,
                items_written = ?,
                error_text = ?,
                metadata_json = COALESCE(?, metadata_json)
            WHERE id = ?
            """,
            (
                utc_now(),
                status,
                items_seen,
                items_written,
                error_text,
                _json(metadata) if metadata else None,
                run_id,
            ),
        )


def store_artifact_from_path(
    config: ChronicleConfig,
    *,
    source_path: Path,
    artifact_type: str,
    observed_at_utc: str | None = None,
    entity_type: str | None = None,
    entity_name: str | None = None,
    entity_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not source_path.exists() or not source_path.is_file():
        return None

    observed_at = observed_at_utc or utc_now()
    source_size = source_path.stat().st_size
    sha256 = _file_sha256(source_path)
    mime_type = mimetypes.guess_type(source_path.name)[0]
    artifact_metadata = dict(metadata or {})
    artifact_metadata.setdefault("source_name", source_path.name)
    artifact_metadata.setdefault("source_size", source_size)

    storage_mode, skip_reason = _artifact_decision(
        config,
        source_path=source_path,
        source_size=source_size,
    )
    if storage_mode == "skip":
        return None

    if storage_mode == "copy":
        relative_storage = Path(artifact_type) / sha256[:2] / sha256[2:4] / f"{sha256}-{source_path.name}"
        storage_path = config.artifact_dir / relative_storage
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        if not storage_path.exists():
            shutil.copy2(source_path, storage_path)
        resolved_storage_path = str(storage_path)
    else:
        resolved_storage_path = _pointer_storage_path(artifact_type, sha256, source_path.name)
        artifact_metadata["storage_mode"] = "pointer_only"
        artifact_metadata["pointer_reason"] = skip_reason

    with open_connection(config) as connection, connection:
        resolved_entity_id = entity_id
        if entity_type and entity_name and not resolved_entity_id:
            resolved_entity_id, _ = _ensure_entity(
                connection,
                entity_type,
                entity_name,
                observed_at,
                metadata={"source_path": str(source_path)},
            )

        row = _store_artifact_record(
            connection,
            artifact_id=str(uuid.uuid4()),
            artifact_type=artifact_type,
            sha256=sha256,
            storage_path=resolved_storage_path,
            source_path=str(source_path),
            mime_type=mime_type,
            observed_at=observed_at,
            resolved_entity_id=resolved_entity_id,
            artifact_metadata=artifact_metadata,
        )

    return {
        "id": row["id"],
        "artifact_type": row["artifact_type"],
        "sha256": row["sha256"],
        "storage_path": row["storage_path"],
        "source_path": str(source_path),
        "entity_id": row["entity_id"],
    }


def store_artifact_text(
    config: ChronicleConfig,
    *,
    artifact_type: str,
    content: str,
    filename: str,
    observed_at_utc: str | None = None,
    entity_type: str | None = None,
    entity_name: str | None = None,
    entity_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    observed_at = observed_at_utc or utc_now()
    payload = content.encode("utf-8")
    sha256 = _bytes_sha256(payload)
    if len(payload) > config.artifact_max_copy_bytes:
        raise ValueError(
            f"Generated artifact `{filename}` exceeds max copy size of {config.artifact_max_copy_bytes} bytes."
        )
    relative_storage = Path(artifact_type) / sha256[:2] / sha256[2:4] / f"{sha256}-{filename}"
    storage_path = config.artifact_dir / relative_storage
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    if not storage_path.exists():
        storage_path.write_bytes(payload)

    artifact_metadata = dict(metadata or {})
    artifact_metadata.setdefault("source_name", filename)
    artifact_metadata.setdefault("source_size", len(payload))

    with open_connection(config) as connection, connection:
        resolved_entity_id = entity_id
        if entity_type and entity_name and not resolved_entity_id:
            resolved_entity_id, _ = _ensure_entity(
                connection,
                entity_type,
                entity_name,
                observed_at,
                metadata={"generated": True},
            )

        row = _store_artifact_record(
            connection,
            artifact_id=str(uuid.uuid4()),
            artifact_type=artifact_type,
            sha256=sha256,
            storage_path=str(storage_path),
            source_path=None,
            mime_type=mimetypes.guess_type(filename)[0] or "text/plain",
            observed_at=observed_at,
            resolved_entity_id=resolved_entity_id,
            artifact_metadata=artifact_metadata,
        )

    return {
        "id": row["id"],
        "artifact_type": row["artifact_type"],
        "sha256": row["sha256"],
        "storage_path": row["storage_path"],
        "source_path": None,
        "entity_id": row["entity_id"],
    }


def link_artifact(
    config: ChronicleConfig,
    *,
    artifact_id: str,
    target_type: str,
    target_id: str,
    link_role: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    link_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        existing = connection.execute(
            """
            SELECT id
            FROM artifact_links
            WHERE artifact_id = ? AND target_type = ? AND target_id = ? AND link_role = ?
            LIMIT 1
            """,
            (artifact_id, target_type, target_id, link_role),
        ).fetchone()
        if existing:
            link_id = existing["id"]
        connection.execute(
            """
            INSERT INTO artifact_links(
                id,
                artifact_id,
                target_type,
                target_id,
                link_role,
                created_at_utc,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(artifact_id, target_type, target_id, link_role) DO UPDATE SET
                metadata_json = COALESCE(excluded.metadata_json, artifact_links.metadata_json)
            """,
            (
                link_id,
                artifact_id,
                target_type,
                target_id,
                link_role,
                utc_now(),
                _json(metadata) if metadata else None,
            ),
        )
    return link_id


def upsert_relation(
    config: ChronicleConfig,
    *,
    from_entity_type: str,
    from_entity_name: str,
    relation_type: str,
    to_entity_type: str,
    to_entity_name: str,
    rationale: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    created_at = utc_now()
    with open_connection(config) as connection, connection:
        from_entity_id, _ = _ensure_entity(connection, from_entity_type, from_entity_name, created_at)
        to_entity_id, _ = _ensure_entity(connection, to_entity_type, to_entity_name, created_at)
        existing = connection.execute(
            """
            SELECT id
            FROM relations
            WHERE from_entity_id = ? AND relation_type = ? AND to_entity_id = ?
            LIMIT 1
            """,
            (from_entity_id, relation_type, to_entity_id),
        ).fetchone()
        relation_id = existing["id"] if existing else str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO relations(
                id,
                from_entity_id,
                relation_type,
                to_entity_id,
                rationale,
                metadata_json,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(from_entity_id, relation_type, to_entity_id) DO UPDATE SET
                rationale = COALESCE(excluded.rationale, relations.rationale),
                metadata_json = COALESCE(excluded.metadata_json, relations.metadata_json)
            """,
            (
                relation_id,
                from_entity_id,
                relation_type,
                to_entity_id,
                rationale,
                _json(metadata) if metadata else None,
                created_at,
            ),
        )
    return {
        "id": relation_id,
        "from_entity_id": from_entity_id,
        "relation_type": relation_type,
        "to_entity_id": to_entity_id,
    }


def fetch_relations_for_entity(
    config: ChronicleConfig,
    *,
    entity_id: str,
) -> list[dict[str, Any]]:
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT
                r.id,
                r.from_entity_id,
                r.relation_type,
                r.to_entity_id,
                r.rationale,
                r.metadata_json,
                src.name AS from_name,
                dst.name AS to_name
            FROM relations AS r
            JOIN entities AS src
                ON src.id = r.from_entity_id
            JOIN entities AS dst
                ON dst.id = r.to_entity_id
            WHERE r.from_entity_id = ? OR r.to_entity_id = ?
            ORDER BY r.created_at_utc DESC
            """,
            (entity_id, entity_id),
        ).fetchall()
    return [
        {
            "id": row["id"],
            "from_entity_id": row["from_entity_id"],
            "from_name": row["from_name"],
            "relation_type": row["relation_type"],
            "to_entity_id": row["to_entity_id"],
            "to_name": row["to_name"],
            "rationale": row["rationale"],
            "metadata": _load_json(row["metadata_json"]),
        }
        for row in rows
    ]


def store_projection_run(
    config: ChronicleConfig,
    *,
    projection_name: str,
    target_path: Path,
    snapshot_id: str | None,
    status: str,
    content_sha256: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO projection_runs(
                id,
                projection_name,
                target_path,
                snapshot_id,
                rendered_at_utc,
                status,
                content_sha256,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                projection_name,
                str(target_path),
                snapshot_id,
                utc_now(),
                status,
                content_sha256,
                _json(metadata) if metadata else None,
            ),
        )
    return run_id


def fetch_latest_projection_run(
    config: ChronicleConfig,
    *,
    projection_name: str | None = None,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT id, projection_name, target_path, snapshot_id, rendered_at_utc, status, content_sha256, metadata_json
            FROM projection_runs
            WHERE (? IS NULL OR projection_name = ?)
            ORDER BY rendered_at_utc DESC
            LIMIT 1
            """,
            (projection_name, projection_name),
        ).fetchone()
    if not row:
        return None
    return dict(row) | {"metadata": _load_json(row["metadata_json"])}


def start_automation_run(
    config: ChronicleConfig,
    *,
    job_name: str,
    trigger_source: str = "manual",
    run_key: str | None = None,
    details: dict[str, Any] | None = None,
    retry_failed: bool = False,
) -> tuple[str, bool]:
    run_id = str(uuid.uuid4())
    started_at = utc_now()
    with open_connection(config) as connection, connection:
        if run_key:
            existing = connection.execute(
                """
                SELECT id, status, details_json, finished_at_utc
                FROM automation_runs
                WHERE job_name = ? AND run_key = ?
                LIMIT 1
                """,
                (job_name, run_key),
            ).fetchone()
            if existing:
                if retry_failed and existing["status"] in {"failed", "failed_soft"}:
                    retry_details = {
                        "retry": {
                            "previous_status": existing["status"],
                            "previous_finished_at_utc": existing["finished_at_utc"],
                            "retried_at_utc": started_at,
                        }
                    }
                    if details:
                        retry_details.update(details)
                    connection.execute(
                        """
                        UPDATE automation_runs
                        SET trigger_source = ?,
                            started_at_utc = ?,
                            finished_at_utc = NULL,
                            status = 'running',
                            details_json = ?,
                            event_id = NULL,
                            snapshot_id = NULL
                        WHERE id = ?
                        """,
                        (trigger_source, started_at, _json(retry_details), existing["id"]),
                    )
                    return existing["id"], True
                return existing["id"], False
        connection.execute(
            """
            INSERT INTO automation_runs(
                id,
                job_name,
                run_key,
                trigger_source,
                started_at_utc,
                status,
                details_json
            )
            VALUES (?, ?, ?, ?, ?, 'running', ?)
            """,
            (run_id, job_name, run_key, trigger_source, started_at, _json(details) if details else None),
        )
    return run_id, True


def finish_automation_run(
    config: ChronicleConfig,
    *,
    run_id: str,
    status: str,
    details: dict[str, Any] | None = None,
    event_id: str | None = None,
    snapshot_id: str | None = None,
) -> None:
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            UPDATE automation_runs
            SET finished_at_utc = ?,
                status = ?,
                details_json = COALESCE(?, details_json),
                event_id = COALESCE(?, event_id),
                snapshot_id = COALESCE(?, snapshot_id)
            WHERE id = ?
            """,
            (utc_now(), status, _json(details) if details else None, event_id, snapshot_id, run_id),
        )


def fetch_automation_run(
    config: ChronicleConfig,
    *,
    job_name: str,
    run_key: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM automation_runs
            WHERE job_name = ? AND run_key = ?
            LIMIT 1
            """,
            (job_name, run_key),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["details"] = _load_json(row["details_json"])
    return payload


def fetch_latest_automation_run(
    config: ChronicleConfig,
    *,
    job_name: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM automation_runs
            WHERE job_name = ?
            ORDER BY started_at_utc DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["details"] = _load_json(row["details_json"])
    return payload


def start_backup_run(
    config: ChronicleConfig,
    *,
    target_root: str,
) -> str:
    run_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO backup_runs(
                id,
                started_at_utc,
                status,
                target_root
            )
            VALUES (?, ?, 'running', ?)
            """,
            (run_id, utc_now(), target_root),
        )
    return run_id


def finish_backup_run(
    config: ChronicleConfig,
    *,
    run_id: str,
    status: str,
    backup_path: str | None = None,
    manifest_path: str | None = None,
    db_sha256: str | None = None,
    artifact_files: int = 0,
    artifact_bytes: int = 0,
    restore_ok: bool | None = None,
    snapshot_id: str | None = None,
    restore_details: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            UPDATE backup_runs
            SET finished_at_utc = ?,
                status = ?,
                backup_path = COALESCE(?, backup_path),
                manifest_path = COALESCE(?, manifest_path),
                db_sha256 = COALESCE(?, db_sha256),
                artifact_files = ?,
                artifact_bytes = ?,
                restore_ok = ?,
                snapshot_id = COALESCE(?, snapshot_id),
                restore_details_json = COALESCE(?, restore_details_json),
                error_text = ?
            WHERE id = ?
            """,
            (
                utc_now(),
                status,
                backup_path,
                manifest_path,
                db_sha256,
                artifact_files,
                artifact_bytes,
                None if restore_ok is None else int(restore_ok),
                snapshot_id,
                _json(restore_details) if restore_details else None,
                error_text,
                run_id,
            ),
        )


def fetch_latest_backup_run(
    config: ChronicleConfig,
    *,
    successful_only: bool = False,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM backup_runs
            WHERE (? = 0 OR status = 'ok')
            ORDER BY started_at_utc DESC
            LIMIT 1
            """,
            (1 if successful_only else 0,),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["restore_details"] = _load_json(row["restore_details_json"])
    return payload


def upsert_hook_event(
    config: ChronicleConfig,
    *,
    hook_type: str,
    dedupe_key: str,
    source_ref: str | None,
    status: str,
    payload: dict[str, Any] | None = None,
    event_id: str | None = None,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    with open_connection(config) as connection, connection:
        existing = connection.execute(
            """
            SELECT id, event_id, snapshot_id
            FROM hook_events
            WHERE hook_type = ? AND dedupe_key = ?
            LIMIT 1
            """,
            (hook_type, dedupe_key),
        ).fetchone()
        hook_id = existing["id"] if existing else str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO hook_events(
                id,
                hook_type,
                dedupe_key,
                triggered_at_utc,
                source_ref,
                status,
                payload_json,
                event_id,
                snapshot_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(hook_type, dedupe_key) DO UPDATE SET
                status = excluded.status,
                payload_json = COALESCE(excluded.payload_json, hook_events.payload_json),
                event_id = COALESCE(excluded.event_id, hook_events.event_id),
                snapshot_id = COALESCE(excluded.snapshot_id, hook_events.snapshot_id),
                source_ref = COALESCE(excluded.source_ref, hook_events.source_ref)
            """,
            (
                hook_id,
                hook_type,
                dedupe_key,
                utc_now(),
                source_ref,
                status,
                _json(payload) if payload else None,
                event_id,
                snapshot_id,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM hook_events
            WHERE id = ?
            LIMIT 1
            """,
            (hook_id,),
        ).fetchone()
    payload_row = dict(row)
    payload_row["payload"] = _load_json(row["payload_json"])
    return payload_row


def fetch_hook_event(
    config: ChronicleConfig,
    *,
    hook_type: str,
    dedupe_key: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM hook_events
            WHERE hook_type = ? AND dedupe_key = ?
            LIMIT 1
            """,
            (hook_type, dedupe_key),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["payload"] = _load_json(row["payload_json"])
    return payload


def fetch_latest_hook_event(
    config: ChronicleConfig,
    *,
    hook_type: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM hook_events
            WHERE hook_type = ?
            ORDER BY triggered_at_utc DESC
            LIMIT 1
            """,
            (hook_type,),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["payload"] = _load_json(row["payload_json"])
    return payload


def start_curation_run(
    config: ChronicleConfig,
    *,
    curation_type: str,
    run_key: str,
    model_name: str | None = None,
    prompt_sha256: str | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    run_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        existing = connection.execute(
            """
            SELECT id
            FROM curation_runs
            WHERE curation_type = ? AND run_key = ?
            LIMIT 1
            """,
            (curation_type, run_key),
        ).fetchone()
        if existing:
            return existing["id"], False
        connection.execute(
            """
            INSERT INTO curation_runs(
                id,
                curation_type,
                run_key,
                started_at_utc,
                status,
                model_name,
                prompt_sha256,
                payload_json
            )
            VALUES (?, ?, ?, ?, 'running', ?, ?, ?)
            """,
            (
                run_id,
                curation_type,
                run_key,
                utc_now(),
                model_name,
                prompt_sha256,
                _json(payload) if payload else None,
            ),
        )
    return run_id, True


def finish_curation_run(
    config: ChronicleConfig,
    *,
    run_id: str,
    status: str,
    payload: dict[str, Any] | None = None,
    artifact_id: str | None = None,
    event_id: str | None = None,
    notes: str | None = None,
) -> None:
    with open_connection(config) as connection, connection:
        connection.execute(
            """
            UPDATE curation_runs
            SET finished_at_utc = ?,
                status = ?,
                payload_json = COALESCE(?, payload_json),
                artifact_id = COALESCE(?, artifact_id),
                event_id = COALESCE(?, event_id),
                notes = COALESCE(?, notes)
            WHERE id = ?
            """,
            (
                utc_now(),
                status,
                _json(payload) if payload else None,
                artifact_id,
                event_id,
                notes,
                run_id,
            ),
        )


def fetch_curation_run(
    config: ChronicleConfig,
    *,
    curation_type: str,
    run_key: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM curation_runs
            WHERE curation_type = ? AND run_key = ?
            LIMIT 1
            """,
            (curation_type, run_key),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["payload"] = _load_json(row["payload_json"])
    return payload


def fetch_latest_curation_run(
    config: ChronicleConfig,
    *,
    curation_type: str,
) -> dict[str, Any] | None:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM curation_runs
            WHERE curation_type = ?
            ORDER BY started_at_utc DESC
            LIMIT 1
            """,
            (curation_type,),
        ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["payload"] = _load_json(row["payload_json"])
    return payload


def link_event_external_ref(
    config: ChronicleConfig,
    *,
    event_id: str,
    ref_type: str,
    ref_value: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    ref_id = str(uuid.uuid4())
    with open_connection(config) as connection, connection:
        existing = connection.execute(
            """
            SELECT id
            FROM event_external_refs
            WHERE ref_type = ? AND ref_value = ?
            LIMIT 1
            """,
            (ref_type, ref_value),
        ).fetchone()
        if existing:
            ref_id = existing["id"]
        connection.execute(
            """
            INSERT INTO event_external_refs(
                id,
                event_id,
                ref_type,
                ref_value,
                metadata_json,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(ref_type, ref_value) DO UPDATE SET
                event_id = excluded.event_id,
                metadata_json = COALESCE(excluded.metadata_json, event_external_refs.metadata_json)
            """,
            (ref_id, event_id, ref_type, ref_value, _json(metadata) if metadata else None, utc_now()),
        )
    return ref_id


def count_mem0_outbox(
    config: ChronicleConfig,
    *,
    status: str | None = None,
) -> int:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM mem0_outbox
            WHERE (? IS NULL OR status = ?)
            """,
            (status, status),
        ).fetchone()
    return int(row["count"])


def fetch_mem0_outbox_entries(
    config: ChronicleConfig,
    *,
    statuses: tuple[str, ...] = ("pending", "failed"),
    limit: int = 25,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in statuses)
    query = f"""
        SELECT
            o.id,
            o.event_id,
            o.collection_name,
            o.operation,
            o.status,
            o.attempts,
            o.last_attempt_at_utc,
            o.last_error,
            o.payload_json,
            o.created_at_utc,
            o.synced_at_utc,
            e.payload_json AS event_payload_json,
            e.text AS event_text,
            e.why AS event_why,
            e.category AS event_category,
            e.entity_id
        FROM mem0_outbox AS o
        JOIN events AS e
            ON e.id = o.event_id
        WHERE o.status IN ({placeholders})
          AND o.operation = 'add'
        ORDER BY CASE WHEN o.status = 'pending' THEN 0 ELSE 1 END, o.created_at_utc ASC
        LIMIT ?
    """
    with open_connection(config) as connection:
        rows = connection.execute(query, (*statuses, limit)).fetchall()

    entries: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row["payload_json"])
        event_payload = _load_json(row["event_payload_json"])
        project = event_payload.get("project") or _project_from_entity_id(row["entity_id"])
        entries.append(
            {
                "id": row["id"],
                "event_id": row["event_id"],
                "collection_name": row["collection_name"],
                "operation": row["operation"],
                "status": row["status"],
                "attempts": row["attempts"],
                "last_attempt_at_utc": row["last_attempt_at_utc"],
                "last_error": row["last_error"],
                "created_at_utc": row["created_at_utc"],
                "synced_at_utc": row["synced_at_utc"],
                "payload": payload,
                "text": payload.get("text") or row["event_text"],
                "why": payload.get("why") or row["event_why"],
                "category": payload.get("category") or row["event_category"],
                "project": payload.get("project") or project,
            }
        )
    return entries


def has_snapshot_for_local_date(
    config: ChronicleConfig,
    *,
    domain: str,
    local_date: str,
) -> bool:
    with open_connection(config) as connection:
        row = connection.execute(
            """
            SELECT id
            FROM snapshots
            WHERE domain = ?
              AND substr(captured_at_local, 1, 10) = ?
            LIMIT 1
            """,
            (domain, local_date),
        ).fetchone()
    return row is not None


def timeline_state(
    config: ChronicleConfig,
    *,
    target: datetime,
    domain: str | None = None,
    window_hours: int = 6,
    limit: int = 3,
    visibility: str = "default",
) -> dict[str, Any]:
    resolved_visibility = _normalize_event_visibility(visibility)
    target_utc = target.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with open_connection(config) as connection:
        snapshot_rows = connection.execute(
            """
            SELECT
                captured_at_utc,
                payload_json,
                ABS(unixepoch(captured_at_utc) - unixepoch(?)) AS delta_seconds
            FROM snapshots
            WHERE (? IS NULL OR domain = ?)
            ORDER BY delta_seconds ASC
            LIMIT ?
            """,
            (target_utc, domain, domain, limit),
        ).fetchall()
        event_rows = connection.execute(
            """
            SELECT
                e.id,
                e.occurred_at_utc,
                e.actor,
                e.category,
                e.entity_id,
                e.text,
                e.why,
                e.circumstances,
                e.mem0_status,
                e.mem0_error,
                e.payload_json,
                o.status AS outbox_status,
                o.last_error AS outbox_last_error,
                o.payload_json AS outbox_payload_json,
                o.synced_at_utc AS outbox_synced_at
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE (? = 'raw' OR COALESCE(json_extract(e.payload_json, '$.memory_guard.visibility'), '') != 'raw_only')
              AND ABS(unixepoch(e.occurred_at_utc) - unixepoch(?)) <= ?
              AND """ + _event_domain_where_clause("e") + """
            ORDER BY e.occurred_at_utc ASC
            """,
            (resolved_visibility, target_utc, window_hours * 3600, domain, domain, domain),
        ).fetchall()

    snapshots: list[dict[str, Any]] = []
    for row in snapshot_rows:
        payload = _load_json(row["payload_json"])
        payload.setdefault("captured_at_utc", row["captured_at_utc"])
        payload["delta_seconds"] = int(row["delta_seconds"])
        snapshots.append(payload)

    return {
        "target_utc": target_utc,
        "target_local": target.astimezone(ZoneInfo(config.timezone)).isoformat(timespec="seconds"),
        "domain": domain,
        "nearest_snapshots": snapshots,
        "events": [_event_row_to_entry(row) for row in event_rows],
    }


def update_event_memory_guard(
    config: ChronicleConfig,
    *,
    event_id: str,
    memory_guard: dict[str, Any],
) -> bool:
    with open_connection(config) as connection, connection:
        row = connection.execute(
            """
            SELECT
                e.payload_json,
                o.payload_json AS outbox_payload_json
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE e.id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()
        if not row:
            return False

        event_payload = _load_json(row["payload_json"])
        event_payload["memory_guard"] = memory_guard
        connection.execute(
            """
            UPDATE events
            SET payload_json = ?
            WHERE id = ?
            """,
            (_json(event_payload), event_id),
        )

        if row["outbox_payload_json"] is not None:
            outbox_payload = _load_json(row["outbox_payload_json"])
            outbox_payload["memory_guard"] = memory_guard
            connection.execute(
                """
                UPDATE mem0_outbox
                SET payload_json = ?
                WHERE event_id = ? AND operation = 'add'
                """,
                (_json(outbox_payload), event_id),
            )
    return True


def update_event_mem0_state(
    config: ChronicleConfig,
    *,
    event_id: str,
    mem0_status: str,
    mem0_error: str | None = None,
    mem0_raw: str | None = None,
    mem0_synced_at: str | None = None,
    attempted: bool = True,
) -> bool:
    update_time_utc = utc_now()
    synced_at = mem0_synced_at or (update_time_utc if mem0_status in {"stored", "synced"} else None)
    event_status = _event_mem0_status(mem0_status)

    with open_connection(config) as connection, connection:
        row = connection.execute(
            """
            SELECT
                e.occurred_at_utc,
                e.payload_json,
                o.attempts AS outbox_attempts,
                o.created_at_utc AS outbox_created_at_utc,
                o.last_attempt_at_utc AS outbox_last_attempt_at_utc,
                o.synced_at_utc AS outbox_synced_at_utc,
                o.payload_json AS outbox_payload_json
            FROM events AS e
            LEFT JOIN mem0_outbox AS o
                ON o.event_id = e.id AND o.operation = 'add'
            WHERE e.id = ?
            """,
            (event_id,),
        ).fetchone()
        if not row:
            return False

        payload = _load_json(row["payload_json"])
        if mem0_raw is not None:
            payload["mem0_raw"] = mem0_raw
        if mem0_error is not None:
            payload["mem0_error"] = mem0_error
        else:
            payload.pop("mem0_error", None)
        payload["mem0_status"] = event_status
        if synced_at is not None:
            payload["mem0_synced_at"] = synced_at
        else:
            payload.pop("mem0_synced_at", None)

        connection.execute(
            """
            UPDATE events
            SET mem0_status = ?, mem0_error = ?, payload_json = ?
            WHERE id = ?
            """,
            (
                event_status,
                mem0_error,
                _json(payload),
                event_id,
            ),
        )

        outbox_payload = _load_json(row["outbox_payload_json"])
        outbox_payload.update(
            {
                "text": payload.get("text") or "",
                "why": payload.get("why"),
                "category": payload.get("category"),
                "project": payload.get("project"),
            }
        )
        if mem0_raw is not None:
            outbox_payload["mem0_raw"] = mem0_raw
        if mem0_error is not None:
            outbox_payload["mem0_error"] = mem0_error
        else:
            outbox_payload.pop("mem0_error", None)
        if synced_at is not None:
            outbox_payload["mem0_synced_at"] = synced_at
        else:
            outbox_payload.pop("mem0_synced_at", None)

        current_attempts = row["outbox_attempts"] or 0
        attempts = current_attempts + 1 if attempted else current_attempts or _attempt_count(mem0_status)
        last_attempt_at_utc = update_time_utc if attempted else row["outbox_last_attempt_at_utc"]
        created_at_utc = row["outbox_created_at_utc"] or row["occurred_at_utc"]
        if synced_at is None and not attempted:
            synced_at = row["outbox_synced_at_utc"]

        _upsert_mem0_outbox(
            connection,
            event_id,
            outbox_payload,
            created_at_utc,
            mem0_status,
            mem0_error,
            attempts=attempts,
            last_attempt_at_utc=last_attempt_at_utc,
            synced_at_utc=synced_at,
        )
    return True
