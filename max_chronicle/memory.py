"""Explicit agent observations, task handoffs, and revisioned facts.

This module never infers facts from prose. Callers supply structured assertions;
the transaction preserves their evidence and rejects stale replacements.
"""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .store import config_from_manifest, open_connection

Item = Annotated[str, Field(min_length=1, max_length=1000)]


class Checkpoint(BaseModel):
    """Working state another agent can resume without the previous conversation."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    goal: str = Field(
        min_length=1, max_length=2000, description="What this task should accomplish."
    )
    completed: list[Item] = Field(
        default_factory=list, max_length=20, description="Work already completed."
    )
    verification: list[Item] = Field(
        default_factory=list,
        max_length=20,
        description="Checks actually run and their outcomes.",
    )
    open_questions: list[Item] = Field(
        default_factory=list,
        max_length=20,
        description="Unresolved questions, blockers, or uncertainty.",
    )
    next_steps: list[Item] = Field(
        default_factory=list,
        max_length=20,
        description="Concrete steps for the next agent.",
    )


class FactInput(BaseModel):
    """An explicit scoped assertion; supersedes is an optimistic revision check."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    slot: str = Field(
        min_length=1,
        max_length=200,
        description="Stable knowledge key, e.g. project.status or deployment.target.",
    )
    value: str = Field(
        min_length=1,
        max_length=4000,
        description="Asserted value of this knowledge key.",
    )
    kind: Literal["observed", "decision", "assumption"] = Field(
        description="Whether this is observed, chosen, or still an assumption."
    )
    supersedes: str | None = Field(
        default=None,
        max_length=128,
        description="Current fact ID being replaced; required when changing an existing value.",
    )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def validate_entry(entry: dict[str, Any]) -> None:
    for key in ("project", "task_id", "session_id", "request_id"):
        value = entry.get(key)
        if value is not None:
            if not isinstance(value, str) or not value.strip() or len(value) > 200:
                raise ValueError(
                    f"{key} must be a non-empty string of at most 200 characters"
                )
            entry[key] = value.strip()
    if entry.get("task_id") and not entry.get("project"):
        raise ValueError("task_id requires project")
    if entry.get("checkpoint") is not None:
        if not entry.get("project") or not entry.get("task_id"):
            raise ValueError("checkpoint requires project and task_id")
        entry["checkpoint"] = Checkpoint.model_validate(
            entry["checkpoint"]
        ).model_dump()
    if entry.get("fact") is not None:
        entry["fact"] = FactInput.model_validate(entry["fact"]).model_dump()


def request_hash(entry: dict[str, Any]) -> str:
    # A transport reconnect may assign a new session ID to the same retried
    # operation. The original observation keeps its originating session.
    keys = (
        "domain",
        "project",
        "task_id",
        "agent",
        "text",
        "why",
        "category",
        "id",
        "recorded_at",
        "source_files",
        "checkpoint",
        "fact",
    )
    return _hash({key: entry.get(key) for key in keys})


def request_receipt(
    connection: sqlite3.Connection, entry: dict[str, Any]
) -> dict | None:
    if not entry.get("request_id"):
        return None
    row = connection.execute(
        "SELECT * FROM event_observations WHERE request_id = ?", (entry["request_id"],)
    ).fetchone()
    if row is None:
        return None
    if row["request_hash"] != request_hash(entry):
        raise ValueError(
            "request_id already exists with different input; use a new request_id"
        )
    return _observation(row)


def _observation(row) -> dict[str, Any]:
    payload = json.loads(row["payload_json"])
    return {
        "observation_id": row["id"],
        "event_id": row["event_id"],
        "agent": row["actor"],
        "session_id": row["session_id"],
        "project": row["project"],
        "task_id": row["task_id"],
        "domain": row["domain"],
        "recorded_at_utc": row["recorded_at_utc"],
        "text": payload.get("text", ""),
        "why": payload.get("why"),
        "evidence": payload.get("evidence", []),
        "checkpoint": payload.get("checkpoint"),
        "fact_id": payload.get("fact_id"),
    }


def record_observation(
    connection: sqlite3.Connection,
    entry: dict[str, Any],
    event_id: str,
    evidence: list[dict],
) -> dict:
    fingerprint = request_hash(entry)
    observation_hash = _hash(
        [fingerprint, entry.get("session_id"), evidence, entry.get("request_id")]
    )
    previous = connection.execute(
        "SELECT * FROM event_observations WHERE event_id = ? AND observation_hash = ?",
        (event_id, observation_hash),
    ).fetchone()
    if previous is not None:
        return _observation(previous)
    observation_id = str(uuid.uuid4())
    now = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    payload = {**entry, "evidence": evidence}
    if entry.get("fact"):
        payload["fact_id"] = _record_fact(
            connection, entry, observation_id, event_id, evidence, now
        )
    connection.execute(
        """INSERT INTO event_observations(id,event_id,request_id,request_hash,observation_hash,
        domain,project,task_id,session_id,actor,recorded_at_utc,payload_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            observation_id,
            event_id,
            entry.get("request_id"),
            fingerprint,
            observation_hash,
            entry["domain"],
            entry.get("project"),
            entry.get("task_id"),
            entry.get("session_id"),
            entry.get("agent"),
            now,
            _json(payload),
        ),
    )
    return _observation(
        connection.execute(
            "SELECT * FROM event_observations WHERE id = ?", (observation_id,)
        ).fetchone()
    )


def _record_fact(connection, entry, episode_id, event_id, evidence, now) -> str:
    fact = entry["fact"]
    group = _json([entry.get("project"), entry.get("task_id")])
    current = connection.execute(
        "SELECT * FROM facts WHERE domain=? AND group_id=? AND slot_key=? AND status='active' AND expired_at_utc IS NULL",
        (entry["domain"], group, fact["slot"]),
    ).fetchone()
    unchanged = (
        current is not None
        and current["value_key"] == fact["value"]
        and json.loads(current["attributes_json"])["kind"] == fact["kind"]
    )
    expected = fact.get("supersedes")
    if expected and (current is None or current["id"] != expected):
        raise ValueError(
            "supersedes is not the current fact; reload task context before replacing it"
        )
    if current is not None and not unchanged and expected != current["id"]:
        raise ValueError(
            f"Changing this slot requires supersedes=current fact {current['id']}"
        )
    attributes = {
        "project": entry.get("project"),
        "task_id": entry.get("task_id"),
        "kind": fact["kind"],
        "event_id": event_id,
        "observation_id": episode_id,
    }
    connection.execute(
        """INSERT INTO episodes(id,domain,group_id,source,content,content_hash,created_at_utc,valid_at_utc,metadata_json)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            episode_id,
            entry["domain"],
            group,
            entry.get("agent") or "unknown",
            entry["text"],
            _hash(entry["text"]),
            now,
            now,
            _json({**attributes, "evidence": evidence}),
        ),
    )
    tx = connection.execute(
        """INSERT INTO fact_transactions(domain,operation,actor,episode_id,recorded_at_utc,reason)
        VALUES (?,'record_episode',?,?,?,?)""",
        (entry["domain"], entry.get("agent"), episode_id, now, entry.get("why")),
    ).lastrowid
    if unchanged:
        fact_id = current["id"]
    else:
        fact_id = str(uuid.uuid4())
        connection.execute(
            """INSERT INTO facts(id,domain,group_id,relation,fact_text,fact_hash,recorded_at_utc,
            valid_from_utc,valid_from_precision,created_tx_id,slot_key,value_key,cardinality,attributes_json,attributed_to)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                fact_id,
                entry["domain"],
                group,
                fact["slot"],
                f"{fact['slot']}: {fact['value']}\n{entry['text']}",
                _hash(fact),
                now,
                now,
                "instant",
                tx,
                fact["slot"],
                fact["value"],
                "single",
                _json(attributes),
                entry.get("agent"),
            ),
        )
        if current is not None:
            connection.execute(
                "UPDATE facts SET status='retired', expired_at_utc=?, valid_to_utc=?, expired_tx_id=? WHERE id=?",
                (now, now, tx, current["id"]),
            )
            connection.execute(
                "INSERT INTO fact_supersessions(old_fact_id,new_fact_id,reason,tx_id) VALUES (?,?,'manual',?)",
                (current["id"], fact_id, tx),
            )
        connection.execute(
            """INSERT INTO fact_mutation_log(domain,action,fact_id,previous_fact_id,tx_id,reason,recorded_at_utc)
            VALUES (?,'create',?,?,?,?,?)""",
            (
                entry["domain"],
                fact_id,
                current["id"] if current else None,
                tx,
                entry.get("why"),
                now,
            ),
        )
    connection.execute(
        "INSERT INTO fact_observations(fact_id,episode_id,reference_time_utc,episode_valid_at_utc,extractor_version) VALUES (?,?,?,?,?)",
        (fact_id, episode_id, now, now, "explicit-v1"),
    )
    return fact_id


OPEN_TASK_LIMIT = 5
OPEN_TASK_NEXT_STEPS = 3  # enough to choose a task; its own scope returns the full checkpoint
CLOSED_TASK_STATUSES = ("completed", "cancelled")
_CURSOR_ERROR = "Invalid cursor or cursor belongs to another scope"


def _encode_cursor(scope: list, seq: int) -> str:
    return base64.urlsafe_b64encode(_json({"v": 1, "scope": scope, "seq": seq}).encode()).decode()


def _decode_cursor(value: str, scope: list) -> int:
    try:
        cursor = json.loads(base64.urlsafe_b64decode(value.encode()))
        if cursor["scope"] != scope or cursor["v"] != 1 or type(cursor["seq"]) is not int or cursor["seq"] < 0:
            raise ValueError()
    except Exception as exc:
        raise ValueError(_CURSOR_ERROR) from exc
    return cursor["seq"]


def _open_tasks(connection, where: str, params: tuple, limit: int) -> list[dict]:
    """The latest checkpoint of each task in scope whose task.status is not closed (FR-12).

    A task's status is its newest visible task.status fact in any domain: a
    quarantined completion does not close it, and a later reopening wins.
    """
    closed = ",".join("?" for _ in CLOSED_TASK_STATUSES)
    rows = connection.execute(
        """SELECT * FROM (
            SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.project, o.task_id ORDER BY o.seq DESC) AS latest
            FROM event_observations o JOIN events e ON e.id=o.event_id
            WHERE """ + where + """ AND o.task_id IS NOT NULL
              AND json_type(o.payload_json,'$.checkpoint')='object'
        ) AS c WHERE latest = 1 AND COALESCE((
            SELECT lower(trim(f.value_key)) FROM current_facts f
            JOIN events fe ON fe.id=json_extract(f.attributes_json,'$.event_id')
            WHERE f.slot_key='task.status'
              AND json_extract(f.attributes_json,'$.project') IS c.project
              AND json_extract(f.attributes_json,'$.task_id')=c.task_id
              AND COALESCE(json_extract(fe.payload_json,'$.memory_guard.visibility'),'') != 'raw_only'
            ORDER BY f.recorded_at_utc DESC, f.id DESC LIMIT 1
        ), '') NOT IN (""" + closed + """)
        ORDER BY seq DESC LIMIT ?""",
        (*params, *CLOSED_TASK_STATUSES, limit),
    ).fetchall()
    tasks = []
    for row in rows:
        observation = _observation(row)
        checkpoint = observation["checkpoint"]
        tasks.append({
            key: observation[key] for key in ("project", "task_id", "agent", "recorded_at_utc", "event_id")
        } | {"goal": checkpoint.get("goal"), "next_steps": checkpoint.get("next_steps", [])[:OPEN_TASK_NEXT_STEPS]})
    return tasks


def task_context(
    manifest: dict,
    *,
    domain: str | None,
    project: str | None,
    task_id: str | None,
    since: str | None = None,
    before: str | None = None,
    limit: int = 10,
) -> dict:
    """A resumable observation stream; new confirmations advance its cursor.

    ``since`` pages forward to newer changes, ``before`` back to older ones.
    Only a named task has a checkpoint to resume; without ``task_id`` the
    context lists the open tasks in scope instead (FR-12).
    """
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    if task_id and not project:
        raise ValueError("task_id requires project")
    if since and before:
        raise ValueError("since and before are exclusive: page one way at a time")
    scope = [domain, project, task_id]
    after = _decode_cursor(since, scope) if since else 0
    older_than = _decode_cursor(before, scope) if before else None
    where = """(? IS NULL OR o.domain=?) AND (? IS NULL OR o.project=?) AND (? IS NULL OR o.task_id=?)
        AND COALESCE(json_extract(e.payload_json,'$.memory_guard.visibility'),'') != 'raw_only'
        AND COALESCE(json_extract(o.payload_json,'$.memory_guard.visibility'),'') != 'raw_only'"""
    params = (domain, domain, project, project, task_id, task_id)
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        rows = connection.execute(
            "SELECT o.* FROM event_observations o JOIN events e ON e.id=o.event_id WHERE "
            + where
            + " AND o.seq > ? AND (? IS NULL OR o.seq < ?) ORDER BY o.seq "
            + ("ASC" if since else "DESC")
            + " LIMIT ?",
            (*params, after, older_than, older_than, limit + 1),
        ).fetchall()
        more = bool(since and len(rows) > limit)
        has_older = bool(not since and len(rows) > limit)
        rows = rows[:limit]
        if not since:
            rows = list(reversed(rows))
        # From the page itself, so no change recorded meanwhile is skipped; after
        # an older page, following it forward rereads what is newer.
        next_seq = rows[-1]["seq"] if rows else after
        if since and rows:
            has_older = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM event_observations o JOIN events e ON e.id=o.event_id WHERE "
                + where + " AND o.seq < ?)",
                (*params, rows[0]["seq"]),
            ).fetchone()[0] == 1
        checkpoint_row = None
        if task_id:
            checkpoint_row = connection.execute(
                "SELECT o.* FROM event_observations o JOIN events e ON e.id=o.event_id WHERE "
                + where
                + " AND json_type(o.payload_json,'$.checkpoint')='object' ORDER BY o.seq DESC LIMIT 1",
                params,
            ).fetchone()
        open_tasks = None if task_id else _open_tasks(connection, where, params, OPEN_TASK_LIMIT)
        facts = connection.execute(
            """SELECT f.* FROM current_facts f JOIN events e
            ON e.id=json_extract(f.attributes_json,'$.event_id')
            WHERE (? IS NULL OR f.domain=?)
            AND (? IS NULL OR json_extract(f.attributes_json,'$.project')=?)
            AND (? IS NULL OR json_extract(f.attributes_json,'$.task_id')=?
                OR json_extract(f.attributes_json,'$.task_id') IS NULL)
            AND COALESCE(json_extract(e.payload_json,'$.memory_guard.visibility'),'') != 'raw_only'
            ORDER BY f.recorded_at_utc DESC,f.id LIMIT ?""",
            (*params, limit + 1),
        ).fetchall()
    context = {
        "project": project,
        "task_id": task_id,
        "checkpoint": _observation(checkpoint_row) if checkpoint_row else None,
        "changes": [
            {
                key: value
                for key, value in _observation(row).items()
                if key != "checkpoint"
            }
            for row in rows
        ],
        "has_more": more,
        "cursor": _encode_cursor(scope, next_seq),
        "has_older": has_older,
        "before": _encode_cursor(scope, rows[0]["seq"]) if rows and has_older else None,
        "facts_has_more": len(facts) > limit,
        "current_facts": [
            {
                "id": f["id"],
                "slot": f["slot_key"],
                "value": f["value_key"],
                "text": f["fact_text"],
                "agent": f["attributed_to"],
                **json.loads(f["attributes_json"]),
            }
            for f in facts[:limit]
        ],
    }
    if open_tasks is not None:
        context["open_tasks"] = open_tasks
    return context


def event_provenance(config, event_id: str, *, limit: int = 5) -> dict:
    """Bounded evidence for a recall hit, including independent confirmations."""
    with open_connection(config) as connection:
        rows = connection.execute(
            """SELECT * FROM event_observations WHERE event_id=?
            AND COALESCE(json_extract(payload_json,'$.memory_guard.visibility'),'') != 'raw_only'
            ORDER BY seq DESC LIMIT ?""",
            (event_id, limit),
        ).fetchall()
        facts = connection.execute(
            """SELECT id,slot_key,value_key,status,attributes_json FROM facts
            WHERE json_extract(attributes_json,'$.event_id')=? ORDER BY recorded_at_utc DESC LIMIT ?""",
            (event_id, limit),
        ).fetchall()
    return {
        "observations": [
            {
                key: value
                for key, value in _observation(row).items()
                if key
                not in {
                    "text",
                    "why",
                    "checkpoint",
                    "event_id",
                    "project",
                    "domain",
                    "task_id",
                }
            }
            for row in rows
        ],
        "facts": [
            {
                "id": f["id"],
                "slot": f["slot_key"],
                "value": f["value_key"],
                "status": f["status"],
                "kind": json.loads(f["attributes_json"])["kind"],
            }
            for f in facts
        ],
    }
