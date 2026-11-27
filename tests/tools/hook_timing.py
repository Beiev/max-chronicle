"""Cold start of the brief hook: ten runs with the HTTP server up, ten with it stopped.

Run from the checkout: ``python tests/tools/hook_timing.py [--runs N]``. It makes a
temporary workspace and HOME, starts its own server on a free port, and prints
the median and the worst wall time of each case in milliseconds, measured with
``time.perf_counter`` around each hook process. Nothing outside the temporary
directory is read or written.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[2]
SERVER_START_TIMEOUT_S = 60.0
POLL_S = 0.1
HOOK_TIMEOUT_S = 30.0


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def base_env(home: Path, workspace: Path) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("CHRONICLE_", "MCP_", "OLLAMA_", "CLAUDE_", "CODEX_", "GEMINI_", "XDG_",
                                  "ZCODE_"))}
    env.update({"HOME": str(home), "CHRONICLE_ROOT": str(workspace), "OLLAMA_URL": "http://127.0.0.1:1",
                "PYTHONPATH": str(REPO), "TMPDIR": str(home / "tmp")})
    return env


def make_workspace(root: Path) -> tuple[Path, Path, Path]:
    home, workspace = root / "home", root / "ws"
    (home / "tmp").mkdir(parents=True)
    env = base_env(home, workspace)
    subprocess.run([sys.executable, "-m", "max_chronicle.cli", "init", "--root", str(workspace)], env=env,
                   check=True, capture_output=True)
    manifest = workspace / "SSOT_MANIFEST.toml"
    for index in range(5):
        subprocess.run([sys.executable, "-m", "max_chronicle.cli", "--manifest", str(manifest), "record",
                        f"Timing decision {index}: the hook reads the brief", "--agent", "claude",
                        "--category", "decision", "--project", "demo"], env=env, check=True, capture_output=True)
    return home, workspace, manifest


def start_server(home: Path, workspace: Path, manifest: Path, port: int, log: Path) -> subprocess.Popen:
    env = {**base_env(home, workspace), "MCP_HOST": "127.0.0.1", "MCP_PORT": str(port)}
    handle = open(log, "ab")  # noqa: SIM115 - kept open for the server's lifetime
    process = subprocess.Popen([sys.executable, "-m", "max_chronicle.mcp_server", "--manifest", str(manifest),
                                "--transport", "streamable-http", "--migrate"], env=env, stdout=handle,
                               stderr=handle)
    address = workspace / "chronicle-server.json"
    deadline = time.monotonic() + SERVER_START_TIMEOUT_S
    while time.monotonic() < deadline:
        if address.exists():
            return process
        if process.poll() is not None:
            raise RuntimeError(f"the server exited with {process.returncode}; see {log}")
        time.sleep(POLL_S)
    process.kill()
    raise RuntimeError(f"the server did not write its address within {SERVER_START_TIMEOUT_S} s")


def stop_server(process: subprocess.Popen) -> None:
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def run_hook(home: Path, workspace: Path, manifest: Path, *, agent: str = "codex", budget: int = 4500,
             session: str = "timing") -> tuple[float, subprocess.CompletedProcess]:
    payload = json.dumps({"session_id": session, "hook_event_name": "SessionStart", "cwd": str(workspace)})
    started = time.perf_counter()
    result = subprocess.run([sys.executable, "-m", "max_chronicle.brief_hook", "--manifest", str(manifest),
                             "--agent", agent, "--budget", str(budget)], input=payload, capture_output=True,
                            text=True, env=base_env(home, workspace), timeout=HOOK_TIMEOUT_S)
    return (time.perf_counter() - started) * 1000, result


def measure(home: Path, workspace: Path, manifest: Path, runs: int, label: str) -> dict[str, float]:
    times = []
    for index in range(runs):
        elapsed, result = run_hook(home, workspace, manifest, session=f"{label}-{index}")
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError(f"hook run {label} {index} failed: {result.returncode} {result.stderr[-500:]}")
        times.append(elapsed)
    return {"median_ms": round(statistics.median(times)), "worst_ms": round(max(times)), "runs": runs}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="chronicle-hook-timing-") as raw:
        root = Path(raw)
        home, workspace, manifest = make_workspace(root)
        server = start_server(home, workspace, manifest, free_port(), root / "server.log")
        try:
            run_hook(home, workspace, manifest)  # the server's first brief warms its caches
            up = measure(home, workspace, manifest, args.runs, "up")
        finally:
            stop_server(server)
        down = measure(home, workspace, manifest, args.runs, "down")
    print(f"HOOK COLD START up median {up['median_ms']} ms worst {up['worst_ms']} ms; "
          f"down median {down['median_ms']} ms worst {down['worst_ms']} ms ({args.runs} runs each)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
