"""`chronicle doctor` reports the running installation and fits it on one screen."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
import errno
import json
import subprocess
import sys
from urllib.error import URLError

from max_chronicle import doctor, service
from max_chronicle.automation import load_automation_config
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
    # A disk warning names a path under this test's folder, which says "backup"; how
    # full this machine's disk is says nothing about the backup.
    assert not [warning for warning in runtime["warnings"] if "backup" in warning and "GB free" not in warning]


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


# ---- a server that is down, and why ----

SERVICE = "com.example.chronicle-mcp"


def _launchctl(monkeypatch, *, returncode: int, stdout: str = "") -> list[list[str]]:
    """launchd as the test says it is: `launchctl print` answers with this code and text."""
    calls: list[list[str]] = []

    def fake(args, **kwargs):
        calls.append(list(args))
        return subprocess.CompletedProcess(args, returncode, stdout, "")

    monkeypatch.setattr(doctor.shutil, "which", lambda name: f"/bin/{name}")
    monkeypatch.setattr(doctor.subprocess, "run", fake)
    return calls


def test_a_stdio_installation_has_no_server_to_miss(loaded_manifest, loaded_automation) -> None:
    runtime = doctor.runtime_component(loaded_manifest, loaded_automation, server_url=NOBODY)

    assert runtime["server_expected"] is False and runtime["down"] is False
    assert runtime["service"] is None


def test_an_installation_whose_server_runs_the_jobs_is_down_without_it(loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, scheduler_enabled=True)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["server_expected"] is True and runtime["down"] is True
    assert "the HTTP server is not answering" in runtime["warnings"]


def test_a_service_missing_from_launchd_is_named_with_the_way_back(monkeypatch, loaded_manifest,
                                                                    loaded_automation) -> None:
    """Plist in place and valid, nothing running: two outages of six and eight days looked like this."""
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    automation.launch_agent_dir.mkdir(parents=True, exist_ok=True)
    plist = automation.launch_agent_dir / f"{SERVICE}.plist"
    plist.write_text("<plist/>", encoding="utf-8")
    calls = _launchctl(monkeypatch, returncode=113)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert calls[0][:2] == ["launchctl", "print"] and calls[0][2].endswith(f"/{SERVICE}")
    assert runtime["service"] == {"label": SERVICE, "plist": str(plist), "plist_exists": True,
                                  "loaded": False, "pid": None}
    assert runtime["down"] is True
    warning = next(w for w in runtime["warnings"] if SERVICE in w)
    assert f"launchd does not have {SERVICE} loaded (its plist is in place)" in warning
    assert f"launchctl bootstrap gui/$(id -u) {plist}" in warning and "Login Items" in warning


def test_a_service_whose_plist_is_gone_says_so(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    _launchctl(monkeypatch, returncode=113)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert any("its plist is missing" in w for w in runtime["warnings"])


def test_a_loaded_service_that_does_not_answer_is_a_hang(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    _launchctl(monkeypatch, returncode=0, stdout="gui/501/x = {\n\tstate = running\n\tpid = 4242\n}\n")

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["service"]["loaded"] is True and runtime["service"]["pid"] == 4242
    assert f"although {SERVICE} runs as pid 4242: a hang" in " ".join(runtime["warnings"])


def test_a_loaded_service_with_no_process_points_at_its_log(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    _launchctl(monkeypatch, returncode=0, stdout="gui/501/x = {\n\tstate = waiting\n}\n")

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["service"]["pid"] is None
    assert "no process stays up" in " ".join(runtime["warnings"])


def test_where_launchd_does_not_exist_the_service_is_not_asked(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: None)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["service"] is None
    assert runtime["down"] is True and "the HTTP server is not answering" in runtime["warnings"]


def test_a_launchctl_that_hangs_is_reported_not_raised(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: f"/bin/{name}")

    def hang(args, **kwargs):
        raise subprocess.TimeoutExpired(args, kwargs.get("timeout"))

    monkeypatch.setattr(doctor.subprocess, "run", hang)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["service"]["loaded"] is None and "TimeoutExpired" in runtime["service"]["error"]


def test_a_probe_this_machine_could_not_send_does_not_blame_the_server(monkeypatch, loaded_manifest,
                                                                        loaded_automation) -> None:
    """Out of local ports after 51 days of uptime: connect() failed before it reached a healthy server."""
    def no_local_port(url, timeout=None):
        raise URLError(OSError(errno.EADDRNOTAVAIL, "Can't assign requested address"))

    monkeypatch.setattr(doctor, "urlopen", no_local_port)
    automation = dataclasses.replace(loaded_automation, scheduler_enabled=True)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url="http://127.0.0.1:8000")

    assert runtime["server"]["answering"] is False and runtime["server"]["blind"] is True
    assert runtime["down"] is False
    assert any("could not open a connection to the server" in w and "reboot" in w for w in runtime["warnings"])


def test_launchd_saying_the_service_is_gone_outweighs_a_probe_that_could_not_be_sent(monkeypatch, loaded_manifest,
                                                                                    loaded_automation) -> None:
    def no_local_port(url, timeout=None):
        raise URLError(OSError(errno.EADDRNOTAVAIL, "Can't assign requested address"))

    monkeypatch.setattr(doctor, "urlopen", no_local_port)
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    _launchctl(monkeypatch, returncode=113)

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url="http://127.0.0.1:8000")

    assert runtime["server"]["blind"] is True and runtime["down"] is True
    assert f"launchd does not have {SERVICE} loaded" in " ".join(runtime["warnings"])


def test_a_launchd_that_fails_otherwise_is_not_read_as_a_missing_service(monkeypatch, loaded_manifest,
                                                                         loaded_automation) -> None:
    """Only launchd's own 'no such service' says not loaded; any other failure says nothing about the service."""
    automation = dataclasses.replace(loaded_automation, server_launchd_label=SERVICE)
    monkeypatch.setattr(doctor.shutil, "which", lambda name: f"/bin/{name}")
    monkeypatch.setattr(doctor.subprocess, "run",
                        lambda args, **kwargs: subprocess.CompletedProcess(args, 1, "", "Operation not permitted"))

    runtime = doctor.runtime_component(loaded_manifest, automation, server_url=NOBODY)

    assert runtime["service"]["loaded"] is None
    assert "exited 1: Operation not permitted" in runtime["service"]["error"]
    assert "the HTTP server is not answering" in runtime["warnings"]  # no claim about launchd
    assert "unknown (launchctl print exited 1" in doctor.render_summary({"status": "critical", "components": {"runtime": runtime}})


def test_a_refused_connection_is_not_blind() -> None:
    assert doctor.server_status(NOBODY)["blind"] is False


def test_the_server_s_service_is_named_in_the_automation_config(chronicle_sandbox) -> None:
    path = chronicle_sandbox.automation_path
    assert load_automation_config(path).server_launchd_label is None

    path.write_text(path.read_text(encoding="utf-8") + f'\n[server]\nlaunchd_label = "{SERVICE}"\n', encoding="utf-8")

    assert load_automation_config(path).server_launchd_label == SERVICE


def test_the_summary_says_down_and_shows_the_service() -> None:
    summary = doctor.render_summary({"status": "critical", "components": {"runtime": {
        "package_version": "1.0", "down": True,
        "server": {"answering": False, "error": "URLError: refused"},
        "service": {"label": SERVICE, "loaded": False, "plist_exists": True, "pid": None},
        "warnings": []}}})

    assert "doctor: critical" in summary
    assert "server     DOWN (URLError: refused)" in summary
    assert f"service    {SERVICE}: NOT loaded" in summary


def _doctor_cli(chronicle_sandbox, monkeypatch, *flags: str) -> int:
    from max_chronicle import cli

    monkeypatch.setattr(sys, "argv", [
        "chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
        "--automation-config", str(chronicle_sandbox.automation_path), "doctor", "--server-url", NOBODY, *flags])
    return cli.main()


def test_a_strict_doctor_fails_when_the_server_that_runs_the_jobs_is_down(chronicle_sandbox, loaded_manifest,
                                                                          monkeypatch, capsys) -> None:
    """The doctor was green through a six-day outage: the runtime was reported and never counted."""
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    path = chronicle_sandbox.automation_path
    _doctor_cli(chronicle_sandbox, monkeypatch, "--summary", "--strict")
    quiet = capsys.readouterr().out
    path.write_text(path.read_text(encoding="utf-8") + "\n[scheduler]\nenabled = true\n", encoding="utf-8")

    code = _doctor_cli(chronicle_sandbox, monkeypatch, "--summary", "--strict")
    out = capsys.readouterr().out

    # Without a scheduler the same silent server is a warning: this installation needs none.
    assert "server     not answering" in quiet and "doctor: critical" not in quiet and "server     DOWN" not in quiet
    assert code == 1
    assert out.splitlines()[0].endswith("doctor: critical") and "server     DOWN" in out


def test_the_json_doctor_counts_the_missing_server(chronicle_sandbox, loaded_manifest, monkeypatch, capsys) -> None:
    service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Something"})
    path = chronicle_sandbox.automation_path
    _doctor_cli(chronicle_sandbox, monkeypatch)
    before = json.loads(capsys.readouterr().out)
    path.write_text(path.read_text(encoding="utf-8") + "\n[scheduler]\nenabled = true\n", encoding="utf-8")

    _doctor_cli(chronicle_sandbox, monkeypatch)
    after = json.loads(capsys.readouterr().out)

    def counted_elsewhere(payload: dict) -> int:
        components = payload["components"]
        return int(components["audit"].get("issue_count", 0)) + int(components["launchd"].get("issue_count", 0))

    assert before["components"]["runtime"]["down"] is False
    assert before["issue_count"] == counted_elsewhere(before)
    assert after["status"] == "critical" and after["components"]["runtime"]["down"] is True
    assert after["issue_count"] == counted_elsewhere(after) + 1  # the missing server is one issue more
