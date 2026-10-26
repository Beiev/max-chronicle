"""Run the automation jobs on their schedule from inside the long-running server.

launchd fired each job once at its minute and forgot it. A job missed while
the machine slept was started later by an external watchdog, but the job
named its run after the moment it ran, not the moment it was due: a catch-up
just after midnight took the new day's key, the day's own run then found the
key taken and did nothing, and the next catch-up started over. The weekly
audit, due on Sunday, ran every Monday instead.

Here the server decides, once a minute:

- each job's due occurrence, in the installation's timezone (zoneinfo, so the
  daylight saving switch moves no job);
- the run key is that occurrence (its local date, or its ISO week), so a late
  run fills the slot it was due for and never takes the next one;
- the job runs as its own process (``chronicle automation run --occurrence``),
  its work and memory outside the server, one job at a time;
- a run that failed is tried again after ``RETRY_AFTER``, at most
  ``MAX_ATTEMPTS`` times per occurrence; one that was deferred (a backup disk
  not mounted) is tried every ``RETRY_AFTER`` until it runs, which is what
  launchd's StartOnMount did: the backup follows the disk within half an hour;
- a run still marked running after the stale TTL died without a word (a job
  killed at ``JOB_TIMEOUT_S`` writes no final status): it is started again,
  and the job retires the dead row itself;
- a job that fails before it records a row (a broken config, an import error)
  is paused and limited the same way, from the scheduler's own count;
- an occurrence older than ``MAX_LATENESS`` is left alone: the daybook
  repairs its last three days by itself, and an old capture is worthless.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta, timezone
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

from .automation import AutomationConfig, JobSchedule
from .config import mem0_enabled
from .store import DEFAULT_STALE_RUN_TTL_HOURS, config_from_manifest, fetch_automation_run

_LOGGER = logging.getLogger("max_chronicle.scheduler")

TICK_S = 60.0
RETRY_AFTER = timedelta(minutes=30)
MAX_ATTEMPTS = 6
MAX_LATENESS = timedelta(days=3)
JOB_TIMEOUT_S = 2 * 3600
STALE_RUNNING = timedelta(hours=DEFAULT_STALE_RUN_TTL_HOURS)
# The first tick waits until the server has its port: a second server started
# by mistake exits on the busy port before it can start a job.
FIRST_TICK_DELAY_S = 30.0
WEEKLY_JOBS = {"weekly-audit"}
DONE_STATUSES = {"ok", "skipped", "issues", "failed_soft"}
RETRY_STATUSES = {"failed", "deferred", "stale_failed"}


def job_name(config_key: str) -> str:
    """`daily_capture` (the TOML table) -> `daily-capture` (the job)."""
    return config_key.replace("_", "-")


def _at(day: date, schedule: JobSchedule, zone: ZoneInfo) -> datetime:
    return datetime.combine(day, wall_time(schedule.hour, schedule.minute), tzinfo=zone)


def due_occurrence(schedule: JobSchedule, now: datetime) -> datetime:
    """The most recent moment *schedule* was due, at or before *now* (aware, in its zone)."""
    zone = now.tzinfo
    if not isinstance(zone, ZoneInfo):
        raise ValueError("now must carry a zoneinfo timezone")
    if schedule.weekday is None:
        candidate = _at(now.date(), schedule, zone)
        return candidate if _before(candidate, now) else _at(now.date() - timedelta(days=1), schedule, zone)
    target = 7 if int(schedule.weekday) in (0, 7) else int(schedule.weekday)  # launchd: 0 and 7 are Sunday
    for back in range(0, 8):
        day = now.date() - timedelta(days=back)
        candidate = _at(day, schedule, zone)
        if day.isoweekday() == target and _before(candidate, now):
            return candidate
    raise AssertionError("a weekday recurs within eight days")


def _before(moment: datetime, now: datetime) -> bool:
    """Moments compared as instants: within one zone Python compares wall
    clocks, and in the repeated hour after the autumn change 02:10 CET would
    count as before 02:20 CEST."""
    return moment.astimezone(timezone.utc) <= now.astimezone(timezone.utc)


def occurrence_key(job: str, occurrence: datetime) -> str:
    """The run key of *job* due at *occurrence*: its ISO week for a weekly job, else its date."""
    return occurrence.strftime("%G-W%V") if job in WEEKLY_JOBS else occurrence.strftime("%Y-%m-%d")


@dataclass
class Decision:
    job: str
    occurrence: str
    key: str
    action: str  # run | done | running | wait | gave_up | too_old
    detail: str | None = None
    attempts: int = 0  # attempts so far at this occurrence; a failed row from elsewhere counts as one


def _subprocess_runner(manifest_path: Path, automation_path: Path, log_dir: Path) -> Callable[[str, datetime], int]:
    def run(job: str, occurrence: datetime) -> int:
        command = [
            sys.executable, "-P", "-m", "max_chronicle.cli",
            "--manifest", str(manifest_path), "--automation-config", str(automation_path),
            "automation", "run", "--job", job, "--occurrence", occurrence.isoformat(),
            "--trigger-source", "scheduler",
        ]
        log_dir.mkdir(parents=True, exist_ok=True)
        with open(log_dir / f"{job.replace('-', '_')}.out.log", "a", encoding="utf-8") as log:
            log.write(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} scheduler: "
                      f"{job} for {occurrence.isoformat()}\n")
            log.flush()
            # Its own process group, so a timeout ends the job's children too.
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=os.environ.copy(),
                                       start_new_session=True)
            try:
                return process.wait(timeout=JOB_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise

    return run


class Scheduler:
    """Decide each minute which job is due and run it; one at a time."""

    def __init__(
        self,
        manifest: Callable[[], dict[str, Any]],
        automation: Callable[[], AutomationConfig],
        run_job: Callable[[str, datetime], int],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._manifest = manifest
        self._automation = automation
        self._run_job = run_job
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._attempts: dict[tuple[str, str], int] = {}
        self._last_started: dict[tuple[str, str], datetime] = {}
        self._lock = threading.Lock()
        self._last_tick: str | None = None
        self._last_runs: list[dict[str, Any]] = []

    @classmethod
    def for_installation(cls, manifest_path: Path, automation_path: Path, **kwargs: Any) -> "Scheduler":
        from .native_automation import load_native_automation
        from .runtime_context import load_manifest_cached

        automation = load_native_automation(automation_path)
        return cls(lambda: load_manifest_cached(manifest_path), lambda: load_native_automation(automation_path),
                   _subprocess_runner(manifest_path, automation_path, automation.launchd_log_dir), **kwargs)

    def decide(self) -> list[Decision]:
        manifest = self._manifest()
        automation = self._automation()
        config = config_from_manifest(manifest)
        zone = ZoneInfo(config.timezone)
        clock = self._clock()
        now = clock.astimezone(zone)
        ttl_hours = float(getattr(automation.guards, "stale_run_ttl_hours", DEFAULT_STALE_RUN_TTL_HOURS))
        stale_running = timedelta(hours=ttl_hours if ttl_hours > 0 else DEFAULT_STALE_RUN_TTL_HOURS)
        decisions = []
        for key, schedule in sorted(automation.jobs.items()):
            job = job_name(key)
            if job == "mem0-dump" and not mem0_enabled(manifest):
                continue
            occurrence = due_occurrence(schedule, now)
            run_key = occurrence_key(job, occurrence)
            decision = Decision(job, occurrence.isoformat(), run_key, "run",
                                attempts=self._attempts.get((job, run_key), 0))
            if clock - occurrence > MAX_LATENESS:  # aware datetimes in different zones subtract as instants
                decision.action = "too_old"
                decisions.append(decision)
                continue
            row = fetch_automation_run(config, job_name=job, run_key=run_key)
            status = (row or {}).get("status")
            if status == "running":
                started = _parse((row or {}).get("started_at_utc"))
                if started is not None and clock - started <= stale_running:
                    decision.action = "running"
                    decisions.append(decision)
                    continue
                status = "stale running row"  # died without a word: retried like a failure
            if status in DONE_STATUSES:
                decision.action = "done"
            elif status is not None and status not in RETRY_STATUSES and status != "stale running row":
                decision.action, decision.detail = "done", status
            elif status is not None or decision.attempts:
                # Failed, deferred or dead, or failed before it recorded anything:
                # paused between attempts and, unless deferred, limited.
                if status is not None:
                    decision.attempts = max(decision.attempts, 1)
                    decision.detail = status
                last = _parse((row or {}).get("finished_at_utc")) or self._last_started.get((job, run_key))
                if status != "deferred" and decision.attempts >= MAX_ATTEMPTS:
                    decision.action, decision.detail = "gave_up", f"{decision.attempts} attempts"
                elif last is not None and clock - last < RETRY_AFTER:
                    decision.action, decision.detail = "wait", f"retry after {status or 'a failed start'}"
            decisions.append(decision)
        return decisions

    def tick(self) -> list[Decision]:
        """Decide, then run every due job in turn; the decisions are returned."""
        decisions = self.decide()
        for decision in decisions:
            if decision.action != "run":
                continue
            attempt = decision.attempts + 1
            with self._lock:
                self._attempts[(decision.job, decision.key)] = attempt
                self._last_started[(decision.job, decision.key)] = self._clock()
            started = time.monotonic()
            try:
                code: int | str = self._run_job(decision.job, datetime.fromisoformat(decision.occurrence))
            except subprocess.TimeoutExpired:
                code = "timeout"
            except Exception as exc:  # noqa: BLE001 - one broken job must not stop the others
                code = f"{type(exc).__name__}: {exc}"
            record = {"job": decision.job, "occurrence": decision.occurrence, "attempt": attempt,
                      "exit": code, "seconds": round(time.monotonic() - started, 1)}
            _LOGGER.info("scheduler ran %s", record)
            with self._lock:
                self._last_runs = (self._last_runs + [record])[-10:]
        with self._lock:
            self._last_tick = self._clock().strftime("%Y-%m-%dT%H:%M:%SZ")
        return decisions

    def loop(self) -> None:
        time.sleep(FIRST_TICK_DELAY_S)
        while True:
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - a failed tick is retried next minute
                _LOGGER.exception("scheduler tick failed")
            time.sleep(TICK_S)

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.loop, name="chronicle-scheduler", daemon=True)
        thread.start()
        return thread

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"last_tick": self._last_tick, "last_runs": list(self._last_runs)}


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
