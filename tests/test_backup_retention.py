"""Backups are kept per day, week and month; removal only when turned on."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from max_chronicle.backup_retention import (
    RetentionPolicy,
    find_backups,
    plan_retention,
    retention_report,
)


def _backup(root: Path, moment: datetime, *, manifest: bool = True, suffix: str = "") -> Path:
    path = root / moment.strftime("%Y") / (moment.strftime("%Y-%m-%dT%H-%M-%SZ") + suffix)
    path.mkdir(parents=True)
    (path / "chronicle.db").write_bytes(b"db")
    if manifest:
        (path / "backup-manifest.json").write_text("{}", encoding="utf-8")
    return path


def _every_other_day(root: Path, days: int) -> list[Path]:
    start = datetime(2026, 3, 1, 23, 0, tzinfo=timezone.utc)
    return [_backup(root, start + timedelta(days=2 * i)) for i in range(days // 2)]


def test_only_completed_backups_this_package_wrote_are_found(tmp_path) -> None:
    moment = datetime(2026, 10, 1, 23, 0, tzinfo=timezone.utc)
    good = _backup(tmp_path, moment)
    _backup(tmp_path, moment + timedelta(hours=1), manifest=False)  # unfinished
    (tmp_path / "2026" / "holiday-photos").mkdir()
    (tmp_path / "2026" / ".2026-10-02T00-00-00Z-staging").mkdir()
    (tmp_path / "notes").mkdir()

    assert [backup.path for backup in find_backups(tmp_path)] == [good]


def test_a_run_id_suffix_is_still_a_backup(tmp_path) -> None:
    path = _backup(tmp_path, datetime(2026, 10, 1, 23, 0, tzinfo=timezone.utc), suffix="-0a1b2c3d")

    assert [backup.path for backup in find_backups(tmp_path)] == [path]


def test_policy_keeps_the_newest_per_day_week_and_month(tmp_path) -> None:
    paths = _every_other_day(tmp_path, 240)  # 120 backups over eight months

    plan = plan_retention(find_backups(tmp_path), RetentionPolicy(daily=14, weekly=8, monthly=12))

    kept = {str(p) for p in paths} - set(plan["remove"])
    assert plan["backups"] == 120 and plan["keep"] == len(kept)
    assert str(paths[-1]) in kept  # the newest
    newest_14 = {str(p) for p in paths[-14:]}
    assert newest_14 <= kept
    # every month that has a backup keeps at least one (eight months, under the cap of 12)
    months = {Path(p).name[:7] for p in kept}
    assert len(months) == len({p.name[:7] for p in paths})
    assert plan["keep"] < 30


def test_an_empty_policy_still_keeps_the_newest_backup(tmp_path) -> None:
    paths = _every_other_day(tmp_path, 10)

    plan = plan_retention(find_backups(tmp_path), RetentionPolicy(daily=0, weekly=0, monthly=0))

    assert plan["keep"] == 1 and str(paths[-1]) not in plan["remove"]


def test_periods_are_counted_over_existing_backups_not_the_calendar(tmp_path) -> None:
    """A machine that stopped backing up long ago keeps its last good copies."""
    old = [_backup(tmp_path, datetime(2025, 1, d, 23, 0, tzinfo=timezone.utc)) for d in range(1, 21)]

    plan = plan_retention(find_backups(tmp_path), RetentionPolicy(daily=14, weekly=0, monthly=0))

    assert plan["keep"] == 14
    assert {str(p) for p in old[-14:]}.isdisjoint(plan["remove"])


def test_the_default_only_reports_and_removes_nothing(tmp_path) -> None:
    paths = _every_other_day(tmp_path, 120)

    report = retention_report(tmp_path, RetentionPolicy())

    assert report["applied"] is False and report["would_remove"] > 0
    assert all(path.exists() for path in paths)


def test_prune_removes_exactly_the_planned_backups(tmp_path) -> None:
    paths = _every_other_day(tmp_path, 120)
    foreign = tmp_path / "2026" / "keep-me"
    foreign.mkdir()

    report = retention_report(tmp_path, RetentionPolicy(daily=3, weekly=0, monthly=0), apply=True)

    assert report["result"]["removed"] == len(paths) - 3
    assert [p for p in paths if p.exists()] == paths[-3:]
    assert foreign.exists()


def test_negative_counts_are_rejected() -> None:
    with pytest.raises(ValueError):
        RetentionPolicy.from_mapping({"daily": -1})


def test_a_stamp_that_is_no_real_moment_is_not_a_backup(tmp_path) -> None:
    fake = tmp_path / "2026" / "2026-13-40T99-99-99Z"
    fake.mkdir(parents=True)
    (fake / "backup-manifest.json").write_text("{}", encoding="utf-8")

    assert find_backups(tmp_path) == []
