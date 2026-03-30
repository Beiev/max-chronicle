from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from max_chronicle.config import default_config
from max_chronicle.scaffold import scaffold_workspace


PROJECT_STATUS_ROOT = Path(__file__).resolve().parents[1]


def _cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    merged_env["PYTHONPATH"] = str(PROJECT_STATUS_ROOT) + (
        f":{merged_env['PYTHONPATH']}" if merged_env.get("PYTHONPATH") else ""
    )
    if env:
        merged_env.update(env)
    return subprocess.run(
        [sys.executable, "-m", "max_chronicle.cli", *args],
        capture_output=True,
        text=True,
        check=True,
        env=merged_env,
    )


def test_scaffold_workspace_creates_portable_install_root(tmp_path) -> None:
    root = tmp_path / "portable-chronicle"
    payload = scaffold_workspace(root, timezone_name="UTC")

    manifest_path = root / "SSOT_MANIFEST.toml"
    automation_path = root / "CHRONICLE_AUTOMATION.toml"
    mem0_dump_path = root / "mem0-dump.json"

    assert payload["status"] == "ok"
    assert payload["root"] == str(root)
    assert manifest_path.exists()
    assert automation_path.exists()
    assert (root / "chronicle.db").exists()
    assert str(root) in manifest_path.read_text(encoding="utf-8")
    assert str(root) in automation_path.read_text(encoding="utf-8")
    assert json.loads(mem0_dump_path.read_text(encoding="utf-8"))["total_memories"] == 0


def test_default_config_prefers_chronicle_root_env(monkeypatch, tmp_path) -> None:
    root = tmp_path / "env-root"
    monkeypatch.setenv("CHRONICLE_ROOT", str(root))

    config = default_config()

    assert config.status_root == root
    assert config.manifest_path == root / "SSOT_MANIFEST.toml"
    assert config.automation_path == root / "CHRONICLE_AUTOMATION.toml"
    assert config.db_path == root / "chronicle.db"


def test_cli_uses_manifest_root_without_db_override(tmp_path) -> None:
    root = tmp_path / "cli-install"
    init_result = _cli("init", "--root", str(root), "--timezone", "UTC")
    init_payload = json.loads(init_result.stdout)
    assert init_payload["db_path"] == str(root / "chronicle.db")

    db_path = root / "chronicle.db"
    db_path.unlink()

    migrate_result = _cli("--manifest", str(root / "SSOT_MANIFEST.toml"), "migrate")
    migrate_payload = json.loads(migrate_result.stdout)

    assert migrate_payload["db_path"] == str(root / "chronicle.db")
    assert db_path.exists()
