"""The server's scheduler: due occurrences in the installation's zone, keys from the occurrence."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from max_chronicle.automation import JobSchedule
from max_chronicle.native_automation import run_automation_job
from max_chronicle.scheduler import (MAX_ATTEMPTS, RETRY_AFTER, STALE_RUNNING, Scheduler, due_occurrence,
                                     occurrence_key)
from max_chronicle.store import config_from_manifest, open_connection

WARSAW = ZoneInfo("Europe/Warsaw")
DAILY = JobSchedule(hour=2, minute=20, throttle_seconds=600)
DAYBOOK = JobSchedule(hour=23, minute=15, throttle_seconds=600)
SUNDAY = JobSchedule(hour=6, minute=10, throttle_seconds=1800, weekday=0)


def _at(*args) -> datetime:
    return datetime(*args, tzinfo=WARSAW)


def test_a_daily_job_not_yet_due_today_was_due_yesterday() -> None:
    assert due_occurrence(DAYBOOK, _at(2026, 10, 20, 20, 0)) == _at(2026, 10, 19, 23, 15)
    assert due_occurrence(DAILY, _at(2026, 10, 20, 20, 0)) == _at(2026, 10, 20, 2, 20)


def test_the_occurrence_keeps_the_offset_its_own_date_had_across_dst() -> None:
    """2026-10-25 02:01 CET: last night's 23:15 was still summer time (21:15Z)."""
    occurrence = due_occurrence(DAYBOOK, datetime(2026, 10, 25, 1, 1, tzinfo=timezone.utc).astimezone(WARSAW))

    assert occurrence.astimezone(timezone.utc) == datetime(2026, 10, 24, 21, 15, tzinfo=timezone.utc)


def test_the_weekly_audit_is_due_on_its_sunday_and_keyed_by_that_week() -> None:
    """It ran every Monday: a Monday catch-up took the new week's key."""
    sunday = due_occurrence(SUNDAY, _at(2026, 10, 25, 12, 0))
    monday = due_occurrence(SUNDAY, _at(2026, 10, 26, 7, 0))

    assert sunday == monday == _at(2026, 10, 25, 6, 10)
    assert occurrence_key("weekly-audit", monday) == "2026-W43"


def test_a_late_run_after_midnight_keys_the_day_it_was_due() -> None:
    occurrence = due_occurrence(DAYBOOK, _at(2026, 10, 21, 0, 40))

    assert occurrence_key("daybook", occurrence) == "2026-10-20"


@pytest.fixture()
def scheduler_env(loaded_manifest, loaded_automation):
    """A scheduler over the sandbox with a fake job runner and a frozen clock."""
    automation = replace(loaded_automation, jobs={"daily_capture": DAILY, "weekly_audit": SUNDAY},
                         scheduler_enabled=True)
    clock = {"now": datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)}
    runs: list[tuple[str, datetime]] = []

    def run_job(job: str, occurrence: datetime) -> int:
        runs.append((job, occurrence))
        return 0

    scheduler = Scheduler(lambda: loaded_manifest, lambda: automation, run_job, clock=lambda: clock["now"])
    return scheduler, clock, runs, loaded_manifest, automation


def _actions(decisions) -> dict[str, str]:
    return {decision.job: decision.action for decision in decisions}


def test_a_due_job_without_a_run_is_started_with_its_occurrence(scheduler_env) -> None:
    scheduler, _, runs, manifest, _ = scheduler_env

    decisions = scheduler.tick()

    assert _actions(decisions) == {"daily-capture": "run", "weekly-audit": "run"}
    assert [job for job, _ in runs] == ["daily-capture", "weekly-audit"]
    assert runs[1][1] == _at(2026, 10, 18, 6, 10).astimezone(ZoneInfo(config_from_manifest(manifest).timezone))


def test_a_job_whose_occurrence_ran_is_left_alone(scheduler_env) -> None:
    scheduler, clock, runs, manifest, automation = scheduler_env
    zone = ZoneInfo(config_from_manifest(manifest).timezone)
    occurrence = due_occurrence(DAILY, clock["now"].astimezone(zone))
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute(
            "INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, finished_at_utc, status) "
            "VALUES ('r1', 'daily-capture', ?, 'scheduler', '2026-10-20T00:30:00Z', '2026-10-20T00:31:00Z', 'ok')",
            (occurrence_key("daily-capture", occurrence),))

    assert _actions(scheduler.decide())["daily-capture"] == "done"


def test_a_failed_run_is_retried_after_a_pause_and_given_up_after_its_attempts(scheduler_env) -> None:
    scheduler, clock, runs, manifest, _ = scheduler_env
    zone = ZoneInfo(config_from_manifest(manifest).timezone)
    key = occurrence_key("daily-capture", due_occurrence(DAILY, clock["now"].astimezone(zone)))
    finished = clock["now"] - timedelta(minutes=5)
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute(
            "INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, finished_at_utc, status) "
            "VALUES ('r1', 'daily-capture', ?, 'scheduler', ?, ?, 'failed')",
            (key, finished.strftime("%Y-%m-%dT%H:%M:%SZ"), finished.strftime("%Y-%m-%dT%H:%M:%SZ")))

    assert _actions(scheduler.decide())["daily-capture"] == "wait"
    clock["now"] += RETRY_AFTER
    assert _actions(scheduler.decide())["daily-capture"] == "run"
    for _ in range(MAX_ATTEMPTS):
        scheduler.tick()
    assert _actions(scheduler.decide())["daily-capture"] == "gave_up"


def test_a_deferred_backup_is_retried_until_its_disk_comes_back(scheduler_env) -> None:
    """launchd's StartOnMount backed up a disk plugged in late; the scheduler must not give up on it."""
    scheduler, clock, runs, manifest, _ = scheduler_env
    zone = ZoneInfo(config_from_manifest(manifest).timezone)
    key = occurrence_key("daily-capture", due_occurrence(DAILY, clock["now"].astimezone(zone)))
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute(
            "INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, finished_at_utc, status) "
            "VALUES ('r1', 'daily-capture', ?, 'scheduler', '2026-10-20T00:20:00Z', '2026-10-20T00:20:01Z', 'deferred')",
            (key,))

    for _ in range(MAX_ATTEMPTS + 2):
        scheduler.tick()

    assert _actions(scheduler.decide())["daily-capture"] == "run"


def test_a_run_left_running_past_the_stale_ttl_is_started_again(scheduler_env) -> None:
    """A job killed at its timeout writes no final status; its row must not block the occurrence."""
    scheduler, clock, runs, manifest, _ = scheduler_env
    zone = ZoneInfo(config_from_manifest(manifest).timezone)
    key = occurrence_key("daily-capture", due_occurrence(DAILY, clock["now"].astimezone(zone)))
    started = clock["now"] - timedelta(hours=1)
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute(
            "INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, status) "
            "VALUES ('r1', 'daily-capture', ?, 'scheduler', ?, 'running')",
            (key, started.strftime("%Y-%m-%dT%H:%M:%SZ")))

    assert _actions(scheduler.decide())["daily-capture"] == "running"
    clock["now"] = started + STALE_RUNNING + timedelta(minutes=1)
    assert _actions(scheduler.decide())["daily-capture"] == "run"


def test_an_occurrence_too_old_is_not_run(scheduler_env) -> None:
    scheduler, clock, runs, _, automation = scheduler_env
    automation.jobs.clear()
    automation.jobs["weekly_audit"] = replace(SUNDAY, weekday=5)  # Friday
    clock["now"] = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)  # Thursday: last Friday is 6 days ago

    assert _actions(scheduler.tick()) == {"weekly-audit": "too_old"} and runs == []


def test_a_crashing_job_does_not_stop_the_others(scheduler_env) -> None:
    scheduler, _, runs, _, _ = scheduler_env

    def run_job(job: str, occurrence: datetime) -> int:
        runs.append((job, occurrence))
        if job == "daily-capture":
            raise RuntimeError("boom")
        return 0

    scheduler._run_job = run_job
    scheduler.tick()

    assert [job for job, _ in runs] == ["daily-capture", "weekly-audit"]
    assert scheduler.snapshot()["last_runs"][0]["exit"].startswith("RuntimeError")


def test_the_job_keys_its_run_by_the_occurrence_it_was_given(loaded_manifest, loaded_automation) -> None:
    """A run on Monday for Sunday's audit fills Sunday's week, not Monday's."""
    occurrence = _at(2026, 10, 25, 6, 10)

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="weekly-audit",
                                trigger_source="pytest", occurrence=occurrence)

    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        keys = [row[0] for row in connection.execute(
            "SELECT run_key FROM automation_runs WHERE job_name = 'weekly-audit'")]
    assert result.get("status") != "existing" and keys == ["2026-W43"]


def test_with_the_scheduler_on_launchd_installs_no_job_agents(chronicle_sandbox, loaded_manifest,
                                                             loaded_automation) -> None:
    from max_chronicle.native_automation import install_launchd

    agents = chronicle_sandbox.root / "agents"
    install_launchd(loaded_manifest, replace(loaded_automation, scheduler_enabled=True), agent_dir=agents,
                    runtime_dir=chronicle_sandbox.root / "rt", log_dir=chronicle_sandbox.root / "logs", load_jobs=False)

    assert not list(agents.glob("*.plist"))


def test_turning_the_scheduler_on_retires_the_job_agents_an_earlier_install_left(
        chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    from max_chronicle.native_automation import install_launchd

    agents = chronicle_sandbox.root / "agents"
    runtime = chronicle_sandbox.root / "rt"
    paths = dict(agent_dir=agents, runtime_dir=runtime, log_dir=chronicle_sandbox.root / "logs", load_jobs=False)
    before = install_launchd(loaded_manifest, loaded_automation, **paths)
    after = install_launchd(loaded_manifest, replace(loaded_automation, scheduler_enabled=True), **paths)

    assert before["installed"] and not list(agents.glob("*.plist"))
    assert sorted(after["retired"]) == sorted(item["label"] for item in before["installed"])
    assert len(list((runtime / "retired-job-agents").glob("*.plist"))) == len(before["installed"])
