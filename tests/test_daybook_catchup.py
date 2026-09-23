"""A daybook run delayed past midnight must not lose the day that just ended.

Regression: the scheduled evening run found its day already claimed by the
previous night's post-midnight catch-up (a "skipped" run for a then-empty
day), returned "existing", and the next catch-up again summarised the new,
empty day. No daybook was written for days that had plenty of events.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

import max_chronicle.native_automation as native_automation
from max_chronicle.cli import build_parser
from max_chronicle.native_automation import (
    DAYBOOK_SOURCE_KIND,
    run_automation_job,
    run_daybook,
    run_daybook_catchup,
)
from max_chronicle.service import record_event
from max_chronicle.store import config_from_manifest, open_connection, start_curation_run

DAY = "2026-09-20"  # Europe/Warsaw (UTC+2) in the sandbox manifest
NEXT_DAY = "2026-09-21"
OLD_DAY = "2026-09-10"  # outside the catch-up lookback from NEXT_EVENING
JUST_AFTER_MIDNIGHT = datetime(2026, 9, 20, 22, 5, tzinfo=timezone.utc)  # 00:05 local, NEXT_DAY
NEXT_EVENING = datetime(2026, 9, 21, 21, 15, tzinfo=timezone.utc)  # 23:15 local, NEXT_DAY


def _record(
    manifest: dict, text: str, at: str, *, category: str = "decision", source_kind: str | None = None
) -> None:
    options: dict[str, Any] = {"source_kind": source_kind} if source_kind else {}
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
        **options,
    )


def _daybook_rows(manifest: dict) -> list[sqlite3.Row]:
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        return connection.execute(
            "SELECT run_key, status, payload_json FROM curation_runs WHERE curation_type = 'daybook' ORDER BY started_at_utc"
        ).fetchall()


def _daybook_path(sandbox, day: str) -> Path:
    return sandbox.status_root / "daybooks" / day[:4] / f"{day}.md"


def _execute(manifest: dict, sql: str) -> None:
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute(sql)


def test_catchup_after_midnight_writes_the_day_that_just_ended(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    _record(loaded_manifest, "Shipped the offline importer to beta testers.", "2026-09-20T12:00:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)

    assert result["days"][DAY]["status"] == "ok"
    assert result["days"][NEXT_DAY]["status"] == "skipped"
    assert result["status"] == "ok"
    assert "offline importer" in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_a_skipped_morning_claim_does_not_block_the_evening_run(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    _record(loaded_manifest, "Signed the hosting contract for the demo.", "2026-09-21T18:00:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][NEXT_DAY]["status"] == "ok"
    assert result["days"][DAY]["status"] == "existing"
    assert "hosting contract" in _daybook_path(chronicle_sandbox, NEXT_DAY).read_text(encoding="utf-8")


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
    assert json.loads(rows[DAY]["payload_json"])["event_count"] == 2
    assert "Rolled back the importer" in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_a_swapped_event_regenerates_the_day_at_the_same_count(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    # A guard pass hides the first event and a late one arrives: the day still
    # has one visible event, but not the same one.
    _execute(
        loaded_manifest,
        "UPDATE events SET payload_json = json_set(COALESCE(payload_json, '{}'),"
        " '$.memory_guard.visibility', 'raw_only') WHERE text LIKE 'Chose SQLite%'",
    )
    _record(loaded_manifest, "Moved the demo to the new hosting contract.", "2026-09-20T15:00:00Z")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "ok"
    daybook = _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")
    assert "new hosting contract" in daybook
    assert "SQLite over Postgres" not in daybook


def test_a_run_from_before_fingerprints_is_rewritten_once(loaded_manifest, loaded_automation) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    _execute(
        loaded_manifest,
        f"UPDATE curation_runs SET payload_json = json_remove(payload_json, '$.event_fingerprint') WHERE run_key = '{DAY}'",
    )

    rewritten = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)
    settled = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING + timedelta(minutes=5))

    assert rewritten["days"][DAY]["status"] == "ok"
    assert settled["days"][DAY]["status"] == "existing"


def test_a_day_past_the_render_limit_says_so(
    loaded_manifest, loaded_automation, chronicle_sandbox, monkeypatch
) -> None:
    monkeypatch.setattr(native_automation, "DAYBOOK_EVENT_LIMIT", 2)
    for hour in (8, 9, 10):
        _record(loaded_manifest, f"Checked the importer queue at {hour}:00.", f"2026-09-20T{hour:02d}:00:00Z")

    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)

    assert "Showing the first 2 of 3 events" in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_hiding_every_event_of_a_day_clears_its_daybook(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Pasted the staging password into the team chat.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    _execute(
        loaded_manifest,
        "UPDATE events SET payload_json = json_set(COALESCE(payload_json, '{}'),"
        " '$.memory_guard.visibility', 'raw_only')",
    )

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "skipped"
    assert "staging password" not in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_an_unchanged_day_and_its_own_summary_do_not_regenerate(loaded_manifest, loaded_automation) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    # The scheduled run's own summary lands inside the day, and sync
    # bookkeeping touches the day's events. Neither is news about the day.
    _record(
        loaded_manifest,
        f"Chronicle daybook generated for {DAY}.",
        "2026-09-20T21:30:00Z",
        category="daily_summary",
        source_kind=DAYBOOK_SOURCE_KIND,
    )
    _execute(loaded_manifest, "UPDATE events SET mem0_status = 'synced'")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "existing"
    assert result["days"][DAY]["reason"] == "unchanged"
    assert [row["run_key"] for row in _daybook_rows(loaded_manifest)].count(DAY) == 1


def test_an_agents_daily_summary_is_evidence_of_the_day(
    loaded_manifest, loaded_automation, chronicle_sandbox
) -> None:
    _record(loaded_manifest, "Итоги дня: закрыли релиз импортёра.", "2026-09-20T19:00:00Z", category="daily_summary")

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)

    assert result["days"][DAY]["status"] == "ok"
    assert "закрыли релиз" in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_a_failed_day_is_retried_by_the_next_run(
    loaded_manifest, loaded_automation, chronicle_sandbox, monkeypatch
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    compose = native_automation._compose_daybook

    def _disk_full(*args: Any, **kwargs: Any) -> Any:
        raise OSError("No space left on device")

    monkeypatch.setattr(native_automation, "_compose_daybook", _disk_full)
    first = run_daybook_catchup(loaded_manifest, loaded_automation, now=JUST_AFTER_MIDNIGHT)
    monkeypatch.setattr(native_automation, "_compose_daybook", compose)

    second = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert first["days"][DAY]["status"] == "failed"
    assert second["days"][DAY]["status"] == "ok"
    assert "SQLite over Postgres" in _daybook_path(chronicle_sandbox, DAY).read_text(encoding="utf-8")


def test_one_failing_day_does_not_stop_the_others(loaded_manifest, loaded_automation, monkeypatch) -> None:
    _record(loaded_manifest, "Signed the hosting contract for the demo.", "2026-09-21T18:00:00Z")
    run_one_day = native_automation.run_daybook

    def _locked_on_day(manifest: dict, automation: Any, *, target_date: str | None = None, **kwargs: Any):
        if target_date == DAY:
            raise sqlite3.OperationalError("database is locked")
        return run_one_day(manifest, automation, target_date=target_date, **kwargs)

    monkeypatch.setattr(native_automation, "run_daybook", _locked_on_day)

    result = run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)

    assert result["days"][DAY]["status"] == "failed"
    assert result["days"][DAY]["error"]["type"] == "OperationalError"
    assert result["days"][NEXT_DAY]["status"] == "ok"
    assert result["status"] == "failed"


def test_a_running_claim_is_left_alone(loaded_manifest, loaded_automation) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    running_id, created = start_curation_run(
        config_from_manifest(loaded_manifest), curation_type="daybook", run_key=DAY
    )
    assert created

    result = run_daybook(loaded_manifest, loaded_automation, target_date=DAY, regenerate=True)

    assert result["status"] == "existing"
    assert result["run"]["id"] == running_id
    assert [(row["run_key"], row["status"]) for row in _daybook_rows(loaded_manifest)] == [(DAY, "running")]


def test_the_cli_backfills_an_older_skipped_day(
    loaded_manifest, loaded_automation, chronicle_sandbox, capsys
) -> None:
    assert run_daybook(loaded_manifest, loaded_automation, target_date=OLD_DAY, regenerate=True)["status"] == "skipped"
    _record(loaded_manifest, "Imported the September planning notes.", "2026-09-10T09:00:00Z")
    assert OLD_DAY not in run_daybook_catchup(loaded_manifest, loaded_automation, now=NEXT_EVENING)["days"]
    args = build_parser().parse_args(
        [
            "--manifest",
            str(chronicle_sandbox.manifest_path),
            "--automation-config",
            str(chronicle_sandbox.automation_path),
            "curate",
            "daybook",
            "--date",
            OLD_DAY,
        ]
    )
    capsys.readouterr()

    assert args.handler(args) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "ok"
    assert "September planning notes" in _daybook_path(chronicle_sandbox, OLD_DAY).read_text(encoding="utf-8")


def test_the_daybook_job_reports_the_newest_daybook(
    loaded_manifest, loaded_automation, chronicle_sandbox, monkeypatch
) -> None:
    _record(loaded_manifest, "Chose SQLite over Postgres for the local store.", "2026-09-20T08:00:00Z")
    monkeypatch.setattr(
        native_automation, "_local_now", lambda cfg: JUST_AFTER_MIDNIGHT.astimezone(ZoneInfo(cfg.timezone))
    )

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daybook")

    assert result["status"] == "ok"
    assert result["path"] == str(_daybook_path(chronicle_sandbox, DAY))
    assert result["artifact_id"] and result["event_id"]


def test_the_daybook_job_records_every_invocation(loaded_manifest, loaded_automation, monkeypatch) -> None:
    config = config_from_manifest(loaded_manifest)
    # Two invocations on one local day, within one second of each other.
    moments = iter([JUST_AFTER_MIDNIGHT, JUST_AFTER_MIDNIGHT + timedelta(microseconds=1)])
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
