"""`chronicle doctor` reports the running installation and fits it on one screen."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sys

from max_chronicle import doctor, service
from max_chronicle.store import config_from_manifest, open_connection

NOBODY = "http://127.0.0.1:9"  # discard port: nothing answers


def test_without_a_server_or_a_backup_the_runtime_says_so(loaded_manifest, loaded_automation) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something to count"})

    runtime = doctor.runtime_component(loaded_manifest, loaded_automation, server_url=NOBODY)

    assert runtime["server"]["answering"] is False
    assert runtime["database"]["events"] == 1
    assert runtime["database"]["schema_version"] == runtime["database"]["target_schema_version"]
    assert "the HTTP server is not answering" in runtime["warnings"] and "no good backup" in runtime["warnings"]
    assert runtime["status"] == "warn"


def test_only_configured_jobs_are_listed_and_a_job_never_run_says_so(loaded_manifest, loaded_automation) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("INSERT INTO automation_runs(id, job_name, run_key, started_at_utc, status) "
                           "VALUES ('r1', 'retired-job', 'k', '2026-06-04T05:00:00Z', 'failed')")
        connection.execute("INSERT INTO automation_runs(id, job_name, run_key, started_at_utc, status) "
                           "VALUES ('r2', 'daily-capture', 'k', '2026-10-25T00:00:00Z', 'ok')")

    jobs = {job["job_name"]: job["status"] for job in
            doctor.runtime_component(loaded_manifest, loaded_automation, server_url=NOBODY)["jobs"]}

    assert "retired-job" not in jobs and jobs["daily-capture"] == "ok" and jobs["daybook"] == "never run"


def test_a_recent_good_backup_is_not_a_warning(loaded_manifest, loaded_automation) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    now = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(backup_runs)")}
        values = {"id": "b1", "started_at_utc": (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "status": "ok"}
        values.update({name: "x" for name in ("target_root", "backup_path", "trigger_source") if name in columns})
        connection.execute(f"INSERT INTO backup_runs({', '.join(values)}) VALUES ({', '.join('?' for _ in values)})",
                           tuple(values.values()))

    loaded_automation.backup_root.mkdir(parents=True, exist_ok=True)

    runtime = doctor.runtime_component(loaded_manifest, loaded_automation, server_url=NOBODY, now=now)

    assert runtime["last_good_backup"] is not None
    assert not any("backup" in warning for warning in runtime["warnings"])


def test_the_summary_fits_the_answer_on_one_screen() -> None:
    payload = {
        "status": "ok",
        "components": {
            "audit": {"status": "ok"}, "launchd": {"status": "ok"},
            "runtime": {
                "package_version": "9.9.9",
                "database": {"size_mb": 72.5, "events": 1557, "schema_version": 15, "target_schema_version": 15},
                "server": {"answering": True, "status": "ok", "version": "9.9.9",
                           "loop": {"max_lag_ms_5m": 3},
                           "embedder": {"last_report": {"total_without_embedding": 0}},
                           "scheduler": {"last_tick": "2026-10-26T01:34:56Z",
                                         "last_runs": [{"job": "daybook", "exit": 0}]}},
                "jobs": [{"job_name": "backup", "status": "skipped", "started_at_utc": "2026-10-25T23:50:43Z"}],
                "last_good_backup": "2026-10-23T23:50:58Z",
                "disks": [{"path": "/data", "free_gb": 94.2, "free_ratio": 0.2}],
                "warnings": ["something to look at"],
            },
        },
    }

    text = doctor.render_summary(payload)

    assert text.splitlines()[0] == "Chronicle 9.9.9 · doctor: ok"
    for expected in ("schema 15/15", "loop lag max 3 ms", "last run daybook exit 0", "backup: skipped",
                     "last good 2026-10-23T23:50:58Z", "94.2 GB free (20%)", "audit ok · launchd ok",
                     "warning    something to look at"):
        assert expected in text
    assert len(text.splitlines()) <= 20


def test_doctor_summary_from_the_cli(chronicle_sandbox, loaded_manifest, monkeypatch, capsys) -> None:
    from max_chronicle import cli

    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    monkeypatch.setattr(sys, "argv", [
        "chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
        "--automation-config", str(chronicle_sandbox.automation_path),
        "doctor", "--summary", "--server-url", NOBODY])

    assert cli.main() == 0
    out = capsys.readouterr().out
    assert out.startswith("Chronicle ") and "server     not answering" in out
    assert not out.lstrip().startswith("{")


def test_doctor_json_keeps_its_status_and_adds_the_runtime(chronicle_sandbox, loaded_manifest, monkeypatch,
                                                           capsys) -> None:
    from max_chronicle import cli

    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    monkeypatch.setattr(sys, "argv", [
        "chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
        "--automation-config", str(chronicle_sandbox.automation_path), "doctor", "--server-url", NOBODY])

    cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["components"]["runtime"]["server"]["answering"] is False
    assert payload["status"] in {"ok", "issues", "critical"}


def test_the_server_address_follows_the_server_s_own_variables(monkeypatch) -> None:
    for name in ("CHRONICLE_SERVER_URL", "MCP_HOST", "MCP_PORT"):
        monkeypatch.delenv(name, raising=False)
    assert doctor.default_server_url() == "http://127.0.0.1:8000"
    monkeypatch.setenv("MCP_HOST", "0.0.0.0")
    monkeypatch.setenv("MCP_PORT", "9000")
    assert doctor.default_server_url() == "http://127.0.0.1:9000"
    monkeypatch.setenv("MCP_HOST", "::1")
    assert doctor.default_server_url() == "http://[::1]:9000"
    monkeypatch.setenv("CHRONICLE_SERVER_URL", "http://127.0.0.1:7777/")
    assert doctor.default_server_url() == "http://127.0.0.1:7777"


def _answer(monkeypatch, *, status: int, body: bytes) -> None:
    import io
    from urllib.error import HTTPError

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake(url, timeout):
        if status != 200:
            raise HTTPError(url, status, "status", {}, io.BytesIO(body))
        return Response(body)

    monkeypatch.setattr(doctor, "urlopen", fake)


def test_a_degraded_server_is_a_warning(monkeypatch, loaded_manifest, loaded_automation) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    _answer(monkeypatch, status=503, body=b'{"status": "degraded", "db_ok": false, "error": "disk I/O error"}')

    runtime = doctor.runtime_component(loaded_manifest, loaded_automation, server_url="http://127.0.0.1:1")

    assert runtime["server"]["answering"] is True
    assert any("degraded" in warning and "disk I/O error" in warning for warning in runtime["warnings"])


def test_something_else_on_the_port_is_not_taken_for_chronicle(monkeypatch) -> None:
    for body in (b"<html>404</html>", b"[]", b"null"):
        _answer(monkeypatch, status=200, body=body)
        status = doctor.server_status("http://127.0.0.1:1")
        assert status["answering"] is False and "not a Chronicle server" in status["error"]


def test_a_database_without_its_tables_is_reported_not_raised(chronicle_sandbox, loaded_manifest,
                                                              loaded_automation) -> None:
    import sqlite3

    from max_chronicle.store import config_from_manifest

    path = config_from_manifest(loaded_manifest).db_path
    path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(path).close()  # an empty SQLite file

    runtime = doctor.runtime_component(loaded_manifest, loaded_automation, server_url=NOBODY)

    assert runtime["database"] is None and any("database unreadable" in w for w in runtime["warnings"])


def test_a_missing_backup_root_is_a_warning(loaded_manifest, loaded_automation, tmp_path) -> None:
    from dataclasses import replace

    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    gone = replace(loaded_automation, backup_root=tmp_path / "unplugged")

    runtime = doctor.runtime_component(loaded_manifest, gone, server_url=NOBODY)

    assert any("backup root" in warning and "missing" in warning for warning in runtime["warnings"])
