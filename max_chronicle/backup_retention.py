"""Which backups to keep: the newest of each recent day, week and month.

Backups used to accumulate without end: a backup disk held hundreds of them
after half a year. A retention policy keeps the newest backup of each of
the last ``daily`` days, ``weekly`` ISO weeks and ``monthly`` months that have
one, and always the newest backup overall. Periods are counted over the
backups that exist, not the calendar, so a machine that stopped backing up
for a month still keeps its last good copies.

Removal is opt-in (``prune = true``): until then a backup run only reports
what the policy would remove. Only directories this package wrote are ever
candidates: ``<backup_root>/<YYYY>/<stamp>`` with a backup manifest inside.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
from typing import Any

MANIFEST_NAME = "backup-manifest.json"
_STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)(?:-[0-9a-f]{8})?$")


@dataclass(frozen=True)
class RetentionPolicy:
    daily: int = 14
    weekly: int = 8
    monthly: int = 12
    prune: bool = False  # report only, until an operator turns removal on

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> "RetentionPolicy":
        raw = raw or {}
        policy = cls(
            daily=int(raw.get("daily", cls.daily)),
            weekly=int(raw.get("weekly", cls.weekly)),
            monthly=int(raw.get("monthly", cls.monthly)),
            prune=bool(raw.get("prune", cls.prune)),
        )
        if min(policy.daily, policy.weekly, policy.monthly) < 0:
            raise ValueError("backup retention counts must not be negative")
        return policy


@dataclass(frozen=True)
class BackupDir:
    path: Path
    started_at: datetime


def find_backups(backup_root: Path) -> list[BackupDir]:
    """Completed backups under *backup_root*, newest first."""
    found = []
    if not backup_root.is_dir():
        return found
    for year in backup_root.iterdir():
        if not (year.is_dir() and re.fullmatch(r"\d{4}", year.name)):
            continue
        for entry in year.iterdir():
            match = _STAMP.match(entry.name)
            if not (match and entry.is_dir() and (entry / MANIFEST_NAME).is_file()):
                continue  # a staging directory, a foreign folder, or an unfinished run
            try:
                started = datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
            except ValueError:  # shaped like a stamp, not a real moment (month 13): not ours
                continue
            found.append(BackupDir(entry, started))
    return sorted(found, key=lambda item: (item.started_at, item.path.name), reverse=True)


def _newest_per_period(backups: list[BackupDir], key, count: int) -> set[Path]:
    kept: set[Path] = set()
    periods: set[Any] = set()
    for backup in backups:  # newest first, so the first of each period is its newest
        period = key(backup.started_at)
        if period in periods:
            continue
        if len(periods) >= count:
            break
        periods.add(period)
        kept.add(backup.path)
    return kept


def plan_retention(backups: list[BackupDir], policy: RetentionPolicy) -> dict[str, Any]:
    """Split *backups* (newest first) into the ones to keep and the ones to remove."""
    keep: set[Path] = set()
    if backups:
        keep.add(backups[0].path)
    keep |= _newest_per_period(backups, lambda moment: moment.date(), policy.daily)
    keep |= _newest_per_period(backups, lambda moment: moment.isocalendar()[:2], policy.weekly)
    keep |= _newest_per_period(backups, lambda moment: (moment.year, moment.month), policy.monthly)
    prune = [backup for backup in backups if backup.path not in keep]
    return {
        "policy": {"daily": policy.daily, "weekly": policy.weekly, "monthly": policy.monthly,
                   "prune": policy.prune},
        "backups": len(backups),
        "keep": len(backups) - len(prune),
        "remove": [str(backup.path) for backup in prune],
        "oldest_kept": min((b.started_at for b in backups if b.path in keep), default=None),
    }


def apply_retention(backup_root: Path, plan: dict[str, Any]) -> dict[str, Any]:
    """Remove the backups *plan* lists; only directories `find_backups` recognises."""
    known = {str(backup.path) for backup in find_backups(backup_root)}
    removed, refused, errors = [], [], []
    for path in plan["remove"]:
        if path not in known:
            refused.append(path)
            continue
        try:
            shutil.rmtree(path)
            removed.append(path)
        except OSError as exc:
            errors.append({"path": path, "error": f"{type(exc).__name__}: {exc}"})
    return {"removed": len(removed), "refused": refused, "errors": errors}


def retention_report(backup_root: Path, policy: RetentionPolicy, *, apply: bool | None = None) -> dict[str, Any]:
    """Plan, and remove when *apply* (default: the policy's ``prune``) says so."""
    plan = plan_retention(find_backups(backup_root), policy)
    applying = policy.prune if apply is None else apply
    report = {**plan, "applied": applying}
    if plan["oldest_kept"] is not None:
        report["oldest_kept"] = plan["oldest_kept"].strftime("%Y-%m-%dT%H:%M:%SZ")
    report["would_remove" if not applying else "to_remove"] = len(plan["remove"])
    if applying and plan["remove"]:
        report["result"] = apply_retention(backup_root, plan)
    if not applying:
        report["remove"] = plan["remove"][:20]  # a report, not a dump of every path
    return report
