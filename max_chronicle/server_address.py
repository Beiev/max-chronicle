"""Where the HTTP server answers: the file it keeps in the workspace, and how a client finds it.

The server writes ``chronicle-server.json`` next to the manifest once it
listens, and removes it on a clean stop. A client takes the address from, in
this order: an explicit URL, ``CHRONICLE_SERVER_URL``, that file. There is no
default port. Standard library only: the brief hook imports this before its
HTTP request, when nothing heavy may load yet.

``open_server`` is the one way the package's clients (connect, doctor, the
brief hook) send HTTP to a Chronicle server: a loopback host is reached
directly, never through a proxy from ``http_proxy`` or the system settings.
"""

from __future__ import annotations

from functools import lru_cache
import ipaddress
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import urlsplit
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener, urlopen

ADDRESS_FILE_NAME = "chronicle-server.json"
ENV_SERVER_URL = "CHRONICLE_SERVER_URL"
_WILDCARD_HOSTS = {"", "0.0.0.0", "::", "[::]"}


def address_path(workspace: Path) -> Path:
    return workspace / ADDRESS_FILE_NAME


def base_url(host: str, port: int) -> str:
    """The URL a client on this machine uses for a server bound to *host*:*port*."""
    if host in _WILDCARD_HOSTS:
        host = "127.0.0.1"
    elif ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}"


def write_address(workspace: Path, url: str, *, version: str, pid: int | None = None,
                  started_at: str | None = None) -> Path:
    """Replace the address file atomically; returns its path."""
    path = address_path(workspace)
    payload = {"url": url, "pid": pid if pid is not None else os.getpid(), "version": version,
               "started_at": started_at}
    handle, temp = tempfile.mkstemp(prefix=".chronicle-server.", dir=workspace)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream)
            stream.write("\n")
        os.chmod(temp, 0o644)
        os.replace(temp, path)
    except BaseException:
        Path(temp).unlink(missing_ok=True)
        raise
    return path


def read_address(workspace: Path) -> dict[str, Any] | None:
    """The address file's content, or None when it is absent or unreadable."""
    try:
        payload = json.loads(address_path(workspace).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("url"), str) or not payload["url"]:
        return None
    return payload


def remove_address(workspace: Path, *, pid: int | None = None) -> bool:
    """Remove the address file when it names this process (or *pid*); True when removed."""
    owner = pid if pid is not None else os.getpid()
    current = read_address(workspace)
    if current is None or current.get("pid") != owner:
        return False
    try:
        address_path(workspace).unlink()
    except FileNotFoundError:
        return False
    return True


def resolve_server_url(explicit: str | None, workspace: Path | None) -> tuple[str | None, str]:
    """The server's base URL and where it came from: ``--url``, the environment, the file, or none."""
    if explicit:
        return explicit.rstrip("/"), "--url"
    from_env = os.environ.get(ENV_SERVER_URL, "").strip()
    if from_env:
        return from_env.rstrip("/"), ENV_SERVER_URL
    if workspace is not None:
        recorded = read_address(workspace)
        if recorded:
            return recorded["url"].rstrip("/"), str(address_path(workspace))
    return None, "none"


def is_loopback_url(url: str) -> bool:
    """Whether *url* names this machine: ``localhost`` or a loopback address."""
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@lru_cache(maxsize=1)
def _direct_opener() -> OpenerDirector:
    return build_opener(ProxyHandler({}))


def open_server(target: str | Request, timeout: float):
    """Send an HTTP request to a Chronicle server; a loopback host never goes through a proxy."""
    url = target.full_url if isinstance(target, Request) else target
    if is_loopback_url(url):
        return _direct_opener().open(target, timeout=timeout)
    return urlopen(target, timeout=timeout)  # noqa: S310 - a server address the operator gave
