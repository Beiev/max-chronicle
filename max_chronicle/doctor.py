"""What `chronicle doctor` reports about the running installation, and its one-screen summary.

The audit, source and launchd checks say whether the configuration is sound.
This adds what an operator asks first: is the server answering, are the jobs
running, when was the last good backup, how much disk is left. It changes no
record and repairs nothing; like any SQLite reader it may checkpoint the
write-ahead log when it closes the database.
"""

from __future__ import annotations

from datetime import datetime, timezone
import errno
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from . import __version__, embeddings
from .brief import _read_only
from .automation import JOB_NAMES
from .store import config_from_manifest, target_schema_version

SERVER_TIMEOUT_S = 2.0
SERVER_DEFAULT_PORT = "8000"  # the server's own default (chronicle-mcp without MCP_PORT)
_WILDCARD_HOSTS = {"", "0.0.0.0", "::", "[::]"}
BACKUP_WARN_DAYS = 3
DISK_WARN_RATIO = 0.10
LAUNCHCTL_TIMEOUT_S = 5.0
LAUNCHCTL_NO_SUCH_SERVICE = 113  # `launchctl print` on a label the domain does not have
# connect() failed on this side (no local port, descriptor, buffer or route): the
# request never reached the server, so it says nothing about the server. A Mac
# ran out of local ports after 51 days of uptime and every probe read as "dead".
_LOCAL_ERRNOS = frozenset(
    code for code in (
        getattr(errno, name, None)
        for name in ("EADDRNOTAVAIL", "EADDRINUSE", "EMFILE", "ENFILE", "ENOBUFS", "ENOMEM", "ENETDOWN", "ENETUNREACH")
    ) if code is not None
)


def default_server_url() -> str:
    """CHRONICLE_SERVER_URL, else the address the server itself reads from MCP_HOST and MCP_PORT.

    A server bound to every interface is asked on loopback.
    """
    explicit = os.environ.get("CHRONICLE_SERVER_URL")
    if explicit:
        return explicit.rstrip("/")
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    if host in _WILDCARD_HOSTS:
        host = "127.0.0.1"
    elif ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{os.environ.get('MCP_PORT', SERVER_DEFAULT_PORT)}"


def server_status(url: str) -> dict[str, Any]:
    """The server's own /health, or why it did not answer."""
    try:
        with urlopen(f"{url}/health", timeout=SERVER_TIMEOUT_S) as response:  # noqa: S310 - a local URL
            raw = response.read()
    except HTTPError as exc:  # 503 is a degraded Chronicle; its body still says why
        raw = exc.read()
    except (URLError, OSError, HTTPException) as exc:
        reason = getattr(exc, "reason", exc)  # urllib wraps the OSError
        blind = isinstance(reason, OSError) and reason.errno in _LOCAL_ERRNOS
        return {"answering": False, "error": f"{type(exc).__name__}: {exc}", "blind": blind}
    try:
        body = json.loads(raw.decode("utf-8"))
    except ValueError:
        body = None
    if not isinstance(body, dict) or "status" not in body:
        return {"answering": False, "error": f"something else answers at {url}, not a Chronicle server"}
    return {"answering": True, **body}


def service_status(label: str | None, agent_dir: Path) -> dict[str, Any] | None:
    """Whether launchd has the server's service loaded; None where that cannot be asked.

    A service missing from the domain looks configured (its plist is in place and
    valid) while nothing runs: that state lasted eight days in one outage and six
    in another. KeepAlive does not cover it, and a reboot does not bring back a
    service whose background permission was switched off.
    """
    if not label or not hasattr(os, "getuid") or shutil.which("launchctl") is None:
        return None
    plist = agent_dir / f"{label}.plist"
    status: dict[str, Any] = {"label": label, "plist": str(plist), "plist_exists": plist.exists(),
                              "loaded": None, "pid": None}
    try:
        result = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{label}"],  # noqa: S603, S607
                                capture_output=True, text=True, timeout=LAUNCHCTL_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as exc:
        status["error"] = f"{type(exc).__name__}: {exc}"
        return status
    if result.returncode == 0:
        status["loaded"] = True
        pid = re.search(r"^\s*pid = (\d+)$", result.stdout, re.MULTILINE)
        status["pid"] = int(pid.group(1)) if pid else None
    elif result.returncode == LAUNCHCTL_NO_SUCH_SERVICE:
        status["loaded"] = False
    else:  # launchd could not be asked: that is not an answer about the service
        status["error"] = f"launchctl print exited {result.returncode}: {(result.stderr or result.stdout).strip()[:200]}"
    return status


def _server_warning(server: dict[str, Any], service: dict[str, Any] | None) -> str:
    """Why the server does not answer, as far as this machine can tell."""
    if server.get("blind") and not (service and service.get("loaded") is False):
        return (f"this machine could not open a connection to the server ({server.get('error')}): "
                "the server may be running; a Mac that ran out of local ports needs a reboot")
    if service and service.get("loaded") is False:
        where = "its plist is in place" if service.get("plist_exists") else "its plist is missing"
        return (f"the HTTP server is not answering: launchd does not have {service['label']} loaded ({where}). "
                f"`launchctl bootstrap gui/$(id -u) {service['plist']}` loads it; if that is refused, its "
                "background permission is off (System Settings, Login Items & Extensions)")
    if service and service.get("loaded") and service.get("pid") is None:
        return (f"the HTTP server is not answering: launchd has {service['label']} loaded but no process "
                "stays up; its error log says why")
    if service and service.get("pid"):
        return f"the HTTP server is not answering although {service['label']} runs as pid {service['pid']}: a hang"
    return "the HTTP server is not answering"


def _disk(path: Path) -> dict[str, Any] | None:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    return {"path": str(path), "free_gb": round(usage.free / 2**30, 1), "free_ratio": round(usage.free / usage.total, 3)}


def _age(stamp: str | None, now: datetime) -> float | None:
    if not stamp:
        return None
    try:
        moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)  # stored times are UTC
    return (now - moment).total_seconds() / 86400


def runtime_component(manifest: dict[str, Any], automation: Any, *, server_url: str | None = None,
                      now: datetime | None = None) -> dict[str, Any]:
    """The running installation: server, jobs, backups, database and disks, with what needs a look."""
    now = now or datetime.now(timezone.utc)
    config = config_from_manifest(manifest)
    component: dict[str, Any] = {"package_version": __version__,
                                 "server": server_status(server_url or default_server_url())}
    warnings: list[str] = []
    try:
        with _read_only(config) as connection:
            component["database"] = {
                "path": str(config.db_path),
                "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                "target_schema_version": target_schema_version(config),
                "events": connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                "size_mb": round(config.db_path.stat().st_size / 2**20, 1),
            }
            last_backup = connection.execute(
                "SELECT MAX(started_at_utc) FROM backup_runs WHERE status = 'ok'").fetchone()[0]
            # The configured jobs only: retired ones keep their old rows forever.
            configured = sorted(job for job in (key.replace("_", "-") for key in automation.jobs)
                                if job in JOB_NAMES)
            latest = {row["job_name"]: dict(row) for row in connection.execute(
                """SELECT job_name, status, started_at_utc FROM automation_runs r
                WHERE started_at_utc = (SELECT MAX(started_at_utc) FROM automation_runs WHERE job_name = r.job_name)""")}
            component["jobs"] = [latest.get(job, {"job_name": job, "status": "never run", "started_at_utc": None})
                                 for job in configured]
    except FileNotFoundError:
        component["database"] = None
        warnings.append("no database yet")
        last_backup = None
    except sqlite3.DatabaseError as exc:  # another application's file, or a damaged one
        component["database"] = None
        warnings.append(f"database unreadable: {exc}")
        last_backup = None
    component["last_good_backup"] = last_backup
    backup_age = _age(last_backup, now)
    if component["database"] is not None:
        if backup_age is None:
            warnings.append("no good backup")
        elif backup_age > BACKUP_WARN_DAYS:
            warnings.append(f"last good backup {backup_age:.1f} days ago")
        elif backup_age < 0:
            warnings.append("the last good backup is dated in the future: check the clock")
    if not automation.backup_root.exists():
        warnings.append(f"backup root {automation.backup_root} is missing (disk not mounted?)")
    disks = [disk for disk in (_disk(config.db_path.parent), _disk(automation.backup_root)
                               if automation.backup_root.exists() else None) if disk]
    component["disks"] = disks
    warnings += [f"{disk['path']}: {disk['free_gb']} GB free" for disk in disks if disk["free_ratio"] < DISK_WARN_RATIO]
    component["embedder"] = embeddings.backend_status(manifest)
    if component["embedder"]["settings_error"]:
        warnings.append(f"manifest: {component['embedder']['settings_error']}")
    server = component["server"]
    label = getattr(automation, "server_launchd_label", None)
    component["service"] = service_status(label, automation.launch_agent_dir)
    # An installation whose server runs the jobs, or that names the server's
    # service, expects a server: without one nothing is scheduled, backed up or
    # served. A stdio-only installation has no server to miss.
    component["server_expected"] = bool(getattr(automation, "scheduler_enabled", False) or label)
    # A probe this machine could not send proves nothing, unless launchd itself says the service is gone.
    proven = not server.get("blind") or (component["service"] or {}).get("loaded") is False
    component["down"] = bool(component["server_expected"] and not server.get("answering") and proven)
    if not server.get("answering"):
        warnings.append(_server_warning(server, component["service"]))
    elif server.get("status") != "ok" or server.get("db_ok") is False:
        warnings.append(f"the server is {server.get('status')}: {server.get('error') or 'database not ok'}")
    elif server.get("version") and server["version"] != __version__:
        warnings.append(f"the server runs {server['version']}, the installed package is {__version__}: restart it")
    component["warnings"] = warnings
    component["status"] = "warn" if warnings else "ok"
    return component


def render_summary(payload: dict[str, Any]) -> str:
    """`chronicle doctor --summary`: the doctor's answer on one screen."""
    components = payload.get("components", {})
    runtime = components.get("runtime") or {}
    database = runtime.get("database") or {}
    server = runtime.get("server") or {}
    lines = [f"Chronicle {runtime.get('package_version', '?')} · doctor: {payload.get('status')}"]
    if database:
        lines.append(f"database   {database['size_mb']} MB · {database['events']} events · schema "
                     f"{database['schema_version']}/{database['target_schema_version']}")
    if server.get("answering"):
        loop = server.get("loop") or {}
        embedder = ((server.get("embedder") or {}).get("last_report") or {})
        parts = [f"{server.get('status')} · {server.get('version')}"]
        if loop:
            parts.append(f"loop lag max {loop.get('max_lag_ms_5m')} ms (5 min)")
        if embedder:
            parts.append(f"{embedder.get('total_without_embedding', 0)} events without vectors")
        lines.append("server     " + " · ".join(parts))
        scheduler = server.get("scheduler")
        if scheduler:
            last = (scheduler.get("last_runs") or [{}])[-1]
            lines.append(f"scheduler  last tick {scheduler.get('last_tick')}"
                         + (f" · last run {last.get('job')} exit {last.get('exit')}" if last else ""))
    else:
        lines.append(f"server     {'DOWN' if runtime.get('down') else 'not answering'} "
                     f"({server.get('error', 'unknown')})")
    launchd_service = runtime.get("service")
    if launchd_service:
        if launchd_service.get("loaded"):
            state = f"loaded, pid {launchd_service['pid']}" if launchd_service.get("pid") else "loaded, no process"
        elif launchd_service.get("loaded") is False:
            state = "NOT loaded" + ("" if launchd_service.get("plist_exists") else ", plist missing")
        else:
            state = f"unknown ({launchd_service.get('error', 'launchctl did not answer')})"
        lines.append(f"service    {launchd_service['label']}: {state}")
    ollama = runtime.get("embedder") or {}
    if ollama:
        if ollama.get("up"):
            state = (f"{ollama['model']} loaded until {ollama['expires_at']}" if ollama.get("model_loaded")
                     else f"{ollama['model']} not loaded")
        elif ollama.get("start_command"):
            state = "started on demand by `" + " ".join(ollama["start_command"]) + "`"
        else:
            state = "no start command: search runs on text only"
        started = (server.get("embedder") or {}).get("last_start") if server.get("answering") else None
        if started:
            state += f" · last start {started.get('at')}: {started.get('outcome')}"
        lines.append(f"ollama     {'up' if ollama.get('up') else 'down'} · {state}")
    for job in runtime.get("jobs") or []:
        lines.append(f"job        {job['job_name']}: {job['status']}"
                     + (f" at {job['started_at_utc']}" if job.get("started_at_utc") else ""))
    lines.append(f"backups    last good {runtime.get('last_good_backup') or 'never'}")
    for disk in runtime.get("disks") or []:
        lines.append(f"disk       {disk['path']}: {disk['free_gb']} GB free ({disk['free_ratio']:.0%})")
    lines.append("checks     " + " · ".join(f"{name} {component.get('status')}" for name, component in components.items()
                                           if name != "runtime"))
    for warning in runtime.get("warnings") or []:
        lines.append(f"warning    {warning}")
    return "\n".join(lines)
