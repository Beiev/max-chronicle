"""A daybook run delayed past midnight must not lose the day that just ended.

Regression: the scheduled evening run found its day already claimed by the
previous night's post-midnight catch-up (a "skipped" run for a then-empty
day), returned "existing", and the next catch-up again summarised the new,
empty day. No daybook was written for days that had plenty of events.
"""

from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from zoneinfo import ZoneInfo

import max_chronicle.native_automation as native_automation
from max_chronicle.native_automation import run_automation_job, run_daybook_catchup
from max_chronicle.service import record_event
from max_chronicle.store import config_from_manifest, open_connection

DAY = "2026-09-20"  # Europe/Warsaw (UTC+2) in the sandbox manifest
NEXT_DAY = "2026-09-21"
JUST_AFTER_MIDNIGHT = datetime(2026, 9, 20, 22, 5, tzinfo=timezone.utc)  # 00:05 local, NEXT_DAY
NEXT_EVENING = datetime(2026, 9, 21, 21, 15, tzinfo=timezone.utc)  # 23:15 local, NEXT_DAY


def _record(manifest: dict, text: str, at: str, *, category: str = "decision") -> None:
    record_event(
        manifest,
        {
            "agent": "agent-a",
            "domain": "global",
            "category": category,
            "project": "demo",
            "text": text,
            "why": "Keeps the release on schedule.",
            "recorded_at": at,
        },
    )


def _daybook_rows(manifest: dict) -> list[sqlite3.Row]:
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        return connection.execute(
            "SELECT run_key, status, payload_json FROM curation_runs WHERE curation_type = 'daybook' ORDER BY started_at_utc"
        ).fetchall()


def test_catchup_after_midnight_writes_the_day_that_just_ended(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    _record(loaded_manifest, "Shipped the offline importer to beta testers.", "2026-09-20T12:00:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)

    assert result["days"][DAY]["status"] == "ok"
    assert result["days"][NEXT_DAY]["status"] == "skipped"
    assert result["status"] == "ok"
    daybook = chronicle_sandbox.status_root / "daybooks" / "2026" / f"{DAY}.md"
    assert "offline importer" in daybook.read_text(encoding="utf-8")


def test_a_skipped_morning_claim_does_not_block_the_evening_run(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    _record(loaded_manifest, "Signed the hosting contract for the demo.", "2026-09-21T18:00:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][NEXT_DAY]["status"] == "ok"
    assert result["days"][DAY]["status"] == "existing"
    daybook = chronicle_sandbox.status_root / "daybooks" / "2026" / f"{NEXT_DAY}.md"
    assert "hosting contract" in daybook.read_text(encoding="utf-8")


def test_a_late_event_regenerates_the_day_and_keeps_the_old_run(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    _record(loaded_manifest, "Rolled back the importer after a data loss report.", "2026-09-20T20:30:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "ok"
    rows = {row["run_key"]: row for row in _daybook_rows(loaded_manifest)}
    assert any(key.startswith(f"{DAY}:superseded:") for key in rows)
    assert '"event_count": 2' in rows[DAY]["payload_json"]
    daybook = chronicle_sandbox.status_root / "daybooks" / "2026" / f"{DAY}.md"
    assert "Rolled back the importer" in daybook.read_text(encoding="utf-8")


def test_an_unchanged_day_and_its_own_summary_do_not_regenerate(loaded_manifest, loaded_automation) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    # A daybook summary landing inside the day is not new evidence about it.
    _record(
        loaded_manifest,
        f"Chronicle daybook generated for {DAY}.",
        "2026-09-20T21:30:00Z",
        category="daily_summary",
    )

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "existing"
    assert result["days"][DAY]["reason"] == "unchanged"
    assert [row["run_key"] for row in _daybook_rows(loaded_manifest)].count(DAY) == 1


def test_the_daybook_job_records_every_invocation(loaded_manifest, loaded_automation, monkeypatch) -> None:
    config = config_from_manifest(loaded_manifest)
    moments = iter([JUST_AFTER_MIDNIGHT, NEXT_EVENING])
    monkeypatch.setattr(
        native_automation, "_local_now", lambda cfg: next(moments).astimezone(ZoneInfo(cfg.timezone))
    )

    first = run_automation_job(loaded_manifest, loaded_automation, job_name="daybook")
    second = run_automation_job(loaded_manifest, loaded_automation, job_name="daybook")

    assert first["status"] != "existing" and second["status"] != "existing"
    with open_connection(config) as connection:
        keys = [
            row["run_key"]
            for row in connection.execute("SELECT run_key FROM automation_runs WHERE job_name = 'daybook'")
        ]
    assert len(keys) == 2 and len(set(keys)) == 2
