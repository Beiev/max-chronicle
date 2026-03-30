from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import sqlite3
import uuid
from zoneinfo import ZoneInfo

from .config import ChronicleConfig
from .db import utc_now


def _read_jsonl(path) -> list[dict]:
    if not path.exists():
        return []

    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _parse_iso(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_local_iso(timestamp: str, timezone_name: str) -> str:
    local_tz = ZoneInfo(timezone_name)
    return _parse_iso(timestamp).astimezone(local_tz).isoformat(timespec="seconds")


def _json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _source_hash(path) -> str:
    return sha256(path.read_bytes()).hexdigest()


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


def _ensure_project_entity(
    connection: sqlite3.Connection,
    project: str | None,
    recorded_at_utc: str,
) -> tuple[str | None, str | None]:
    if not project:
        return None, None

    slug = _slugify(project)
    entity_id = f"project:{slug}"
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
        VALUES (?, 'project', ?, ?, 'active', NULL, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            entity_id,
            slug,
            _entity_name(slug),
            _json([slug]),
            _json({"project_key": project}),
            recorded_at_utc,
            recorded_at_utc,
        ),
    )
    return entity_id, "project"


def _snapshot_summary(snapshot: dict) -> str:
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


def import_legacy_ledger(
    connection: sqlite3.Connection,
    config: ChronicleConfig,
    queue_mem0: bool = False,
) -> dict[str, int]:
    ledger_rows = _read_jsonl(config.ledger_path)
    run_id = str(uuid.uuid4())
    source_hash = _source_hash(config.ledger_path) if config.ledger_path.exists() else None
    started_at = utc_now()
    imported = 0
    skipped = 0

    connection.execute(
        """
        INSERT INTO ingest_runs(
            id,
            adapter,
            started_at_utc,
            finished_at_utc,
            status,
            source_ref,
            source_hash,
            items_seen,
            items_written,
            error_text,
            metadata_json
        )
        VALUES (?, 'legacy_ledger', ?, NULL, 'running', ?, ?, 0, 0, NULL, ?)
        """,
        (
            run_id,
            started_at,
            str(config.ledger_path),
            source_hash,
            _json({"queue_mem0": queue_mem0}),
        ),
    )

    try:
        for row in ledger_rows:
            event_id = row.get("id") or str(uuid.uuid4())
            exists = connection.execute(
                "SELECT 1 FROM events WHERE id = ?",
                (event_id,),
            ).fetchone()
            if exists:
                skipped += 1
                continue

            recorded_at_utc = row.get("recorded_at") or utc_now()
            occurred_at_local = _to_local_iso(recorded_at_utc, config.timezone)
            entity_id, entity_type = _ensure_project_entity(
                connection,
                row.get("project"),
                recorded_at_utc,
            )

            category = row.get("category") or "note"
            event_type = f"legacy.{category}"
            connection.execute(
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
                    occurred_at_local,
                    config.timezone,
                    recorded_at_utc,
                    row.get("agent"),
                    event_type,
                    category,
                    entity_type,
                    entity_id,
                    row.get("project"),
                    row.get("text") or "",
                    row.get("why"),
                    row.get("domain"),
                    "legacy_jsonl",
                    str(config.ledger_path),
                    source_hash,
                    _json(row),
                    "ssot-ledger.jsonl",
                    row.get("mem0_status"),
                    row.get("mem0_error"),
                ),
            )

            if queue_mem0 and row.get("category"):
                outbox_status = "synced" if row.get("mem0_status") == "stored" else "pending"
                payload = {
                    "text": row.get("text") or "",
                    "why": row.get("why"),
                    "category": row.get("category"),
                    "project": row.get("project"),
                }
                connection.execute(
                    """
                    INSERT OR IGNORE INTO mem0_outbox(
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
                    VALUES (?, ?, 'napaarnik_personal', 'add', ?, 0, NULL, NULL, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        event_id,
                        outbox_status,
                        _json(payload),
                        started_at,
                        started_at if outbox_status == "synced" else None,
                    ),
                )
            imported += 1

        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at_utc = ?,
                status = 'completed',
                items_seen = ?,
                items_written = ?
            WHERE id = ?
            """,
            (utc_now(), len(ledger_rows), imported, run_id),
        )
    except Exception as exc:
        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at_utc = ?,
                status = 'failed',
                items_seen = ?,
                items_written = ?,
                error_text = ?
            WHERE id = ?
            """,
            (utc_now(), len(ledger_rows), imported, str(exc), run_id),
        )
        raise

    return {"seen": len(ledger_rows), "imported": imported, "skipped": skipped}


def import_legacy_snapshots(
    connection: sqlite3.Connection,
    config: ChronicleConfig,
) -> dict[str, int]:
    snapshot_rows = _read_jsonl(config.snapshot_path)
    run_id = str(uuid.uuid4())
    source_hash = _source_hash(config.snapshot_path) if config.snapshot_path.exists() else None
    imported = 0
    skipped = 0

    connection.execute(
        """
        INSERT INTO ingest_runs(
            id,
            adapter,
            started_at_utc,
            finished_at_utc,
            status,
            source_ref,
            source_hash,
            items_seen,
            items_written,
            error_text,
            metadata_json
        )
        VALUES (?, 'legacy_snapshots', ?, NULL, 'running', ?, ?, 0, 0, NULL, '{}')
        """,
        (
            run_id,
            utc_now(),
            str(config.snapshot_path),
            source_hash,
        ),
    )

    try:
        for row in snapshot_rows:
            snapshot_id = row.get("id") or str(uuid.uuid4())
            exists = connection.execute(
                "SELECT 1 FROM snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()
            if exists:
                skipped += 1
                continue

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
                """,
                (
                    snapshot_id,
                    row.get("captured_at_utc") or utc_now(),
                    row.get("captured_at_local")
                    or _to_local_iso(row.get("captured_at_utc") or utc_now(), config.timezone),
                    row.get("timezone") or config.timezone,
                    row.get("agent"),
                    row.get("domain"),
                    row.get("title"),
                    row.get("focus"),
                    row.get("label"),
                    _snapshot_summary(row),
                    _json(row),
                ),
            )
            imported += 1

        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at_utc = ?,
                status = 'completed',
                items_seen = ?,
                items_written = ?
            WHERE id = ?
            """,
            (utc_now(), len(snapshot_rows), imported, run_id),
        )
    except Exception as exc:
        connection.execute(
            """
            UPDATE ingest_runs
            SET finished_at_utc = ?,
                status = 'failed',
                items_seen = ?,
                items_written = ?,
                error_text = ?
            WHERE id = ?
            """,
            (utc_now(), len(snapshot_rows), imported, str(exc), run_id),
        )
        raise

    return {"seen": len(snapshot_rows), "imported": imported, "skipped": skipped}


def bootstrap_legacy(
    connection: sqlite3.Connection,
    config: ChronicleConfig,
    queue_mem0: bool = False,
) -> dict[str, dict[str, int]]:
    ledger = import_legacy_ledger(connection, config, queue_mem0=queue_mem0)
    snapshots = import_legacy_snapshots(connection, config)
    return {"ledger": ledger, "snapshots": snapshots}
