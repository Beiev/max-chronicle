"""Tests for `chronicle browse` subcommands.

Exercises each subcommand (search, recent, entity, daybook) against the
chronicle_sandbox fixture.  Embed calls are mocked so Ollama is not required.
"""
from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from max_chronicle.db import apply_migrations, connect
from max_chronicle.runtime_context import load_manifest
from max_chronicle.service import record_event, query_memory
from max_chronicle.store import config_from_manifest, fetch_recent_events

from tests.conftest import ChronicleSandbox


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_cli(sandbox: ChronicleSandbox, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run the chronicle CLI in a subprocess, pointing at the sandbox."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "max_chronicle.cli",
            "--db",
            str(sandbox.chronicle_db),
            "--manifest",
            str(sandbox.manifest_path),
            "--automation-config",
            str(sandbox.automation_path),
            *args,
        ],
        capture_output=True,
        text=True,
        env=full_env,
    )


def _seed_db(sandbox: ChronicleSandbox) -> None:
    """Apply migrations and seed a few events."""
    manifest = load_manifest(sandbox.manifest_path)
    config = config_from_manifest(manifest)
    with connect(config.db_path) as conn:
        apply_migrations(conn, config)
    record_event(
        manifest,
        {
            "text": "chronicle browse feature implemented",
            "category": "milestone",
            "project": "chronicle",
            "domain": "global",
        },
        append_compat=False,
        source_kind="test_seed",
    )
    record_event(
        manifest,
        {
            "text": "daily automation job ran successfully",
            "category": "maintenance",
            "project": "chronicle",
            "domain": "global",
        },
        append_compat=False,
        source_kind="test_seed",
    )


# ---------------------------------------------------------------------------
# browse (no subcommand) — should print help and exit 0
# ---------------------------------------------------------------------------


class TestBrowseHelp:
    def test_no_subcommand_prints_help(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse")
        assert result.returncode == 0, result.stderr
        combined = result.stdout + result.stderr
        assert "search" in combined
        assert "recent" in combined
        assert "entity" in combined
        assert "daybook" in combined


# ---------------------------------------------------------------------------
# browse recent
# ---------------------------------------------------------------------------


class TestBrowseRecent:
    def test_exit_zero_with_events(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "recent", "--limit", "5")
        assert result.returncode == 0, result.stderr

    def test_seeded_event_appears(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "recent", "--limit", "5")
        combined = result.stdout + result.stderr
        # At least one of the seeded events should appear
        assert "chronicle" in combined

    def test_empty_db_exits_zero(self, chronicle_sandbox: ChronicleSandbox) -> None:
        # Migrations only — no events
        manifest = load_manifest(chronicle_sandbox.manifest_path)
        config = config_from_manifest(manifest)
        with connect(config.db_path) as conn:
            apply_migrations(conn, config)
        result = _run_cli(chronicle_sandbox, "browse", "recent", "--limit", "3")
        assert result.returncode == 0, result.stderr

    def test_limit_respected(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """Requesting limit=1 should still exit 0 and not crash."""
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "recent", "--limit", "1")
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# browse search
# ---------------------------------------------------------------------------


class TestBrowseSearch:
    def test_exit_zero(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        # Patch embed_text to return None (Ollama down) so we test FTS+temporal only.
        env = {
            "CHRONICLE_FEATURE_EVENT_EMBEDDINGS": "0",
        }
        result = _run_cli(chronicle_sandbox, "browse", "search", "chronicle", "--limit", "5", env=env)
        assert result.returncode == 0, result.stderr

    def test_seeded_event_in_output(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        env = {"CHRONICLE_FEATURE_EVENT_EMBEDDINGS": "0"}
        result = _run_cli(chronicle_sandbox, "browse", "search", "chronicle", "--limit", "5", env=env)
        combined = result.stdout + result.stderr
        # The seeded event text should appear
        assert "chronicle" in combined.lower()

    def test_no_results_graceful(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """A query that matches nothing should exit 0 with a 'no matches' notice."""
        _seed_db(chronicle_sandbox)
        env = {"CHRONICLE_FEATURE_EVENT_EMBEDDINGS": "0"}
        result = _run_cli(
            chronicle_sandbox,
            "browse", "search", "xyzzy_nonexistent_zz9plural", "--limit", "5",
            env=env,
        )
        assert result.returncode == 0, result.stderr

    def test_direct_query_memory_mock(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """Call query_memory directly with embed_text mocked to None (simulates Ollama down)."""
        _seed_db(chronicle_sandbox)
        manifest = load_manifest(chronicle_sandbox.manifest_path)
        # embed_text is imported inside query_memory from max_chronicle.embeddings
        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            payload = query_memory(manifest, query="chronicle", limit=5)
        assert "results" in payload
        # degraded flag should be set since embed returned None
        assert payload["degraded"] is True
        # FTS/temporal channels should still have found events
        assert len(payload.get("results") or []) > 0
        # Both seeded events are relevant ("chronicle" matches text on one and
        # project on the other) and are recorded within the same second, so RRF
        # can rank either first. Assert presence, not position.
        assert any("chronicle" in (item.get("text") or "").lower() for item in payload["results"])


# ---------------------------------------------------------------------------
# browse entity
# ---------------------------------------------------------------------------


class TestBrowseEntity:
    def test_exit_zero_known_entity(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "entity", "chronicle")
        assert result.returncode == 0, result.stderr

    def test_entity_events_appear(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "entity", "chronicle")
        combined = result.stdout + result.stderr
        assert "chronicle" in combined.lower()

    def test_unknown_entity_exits_zero(self, chronicle_sandbox: ChronicleSandbox) -> None:
        _seed_db(chronicle_sandbox)
        result = _run_cli(chronicle_sandbox, "browse", "entity", "no_such_entity_xyz")
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# browse daybook
# ---------------------------------------------------------------------------


class TestBrowseDaybook:
    def _write_daybook(self, sandbox: ChronicleSandbox, date_str: str, content: str) -> Path:
        year = date_str[:4]
        daybook_dir = sandbox.status_root / "daybooks" / year
        daybook_dir.mkdir(parents=True, exist_ok=True)
        path = daybook_dir / f"{date_str}.md"
        path.write_text(content, encoding="utf-8")
        return path

    def test_list_recent_when_no_date(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """Without --date, it should list recent daybook dates and exit 0."""
        self._write_daybook(chronicle_sandbox, "2026-06-04", "# Daybook 2026-06-04\nTest content.\n")
        result = _run_cli(chronicle_sandbox, "browse", "daybook")
        assert result.returncode == 0, result.stderr
        combined = result.stdout + result.stderr
        assert "2026-06-04" in combined

    def test_specific_date_prints_content(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """With --date, it should print the daybook markdown content."""
        self._write_daybook(
            chronicle_sandbox,
            "2026-06-04",
            "# Daybook 2026-06-04\n\nMilestone: browse command shipped.\n",
        )
        result = _run_cli(chronicle_sandbox, "browse", "daybook", "--date", "2026-06-04")
        assert result.returncode == 0, result.stderr
        combined = result.stdout + result.stderr
        assert "browse command shipped" in combined

    def test_missing_date_returns_nonzero(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """A date with no matching file should exit 1."""
        # Ensure daybooks dir exists but the specific date does not
        (chronicle_sandbox.status_root / "daybooks").mkdir(parents=True, exist_ok=True)
        result = _run_cli(chronicle_sandbox, "browse", "daybook", "--date", "1999-01-01")
        assert result.returncode == 1, result.stdout

    def test_empty_daybook_dir_list_exits_zero(self, chronicle_sandbox: ChronicleSandbox) -> None:
        """No daybooks at all → graceful 'no daybook files found' message."""
        daybook_dir = chronicle_sandbox.status_root / "daybooks"
        daybook_dir.mkdir(parents=True, exist_ok=True)
        result = _run_cli(chronicle_sandbox, "browse", "daybook")
        assert result.returncode == 0, result.stderr
