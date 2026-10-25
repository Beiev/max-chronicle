"""Self-observation and background work for the long-running server.

Two things a server shared by many agents needs and a stdio server does not:

- ``LoopMonitor`` measures how late the event loop wakes up. A loop that is
  late cannot answer anyone, and a supervisor that restarts such a server
  destroys the evidence of what it was doing. The monitor logs every lag
  over a second, and when the loop stops
  ticking for ``stall_s`` a separate thread dumps every thread's stack to
  the log, so the cause of a hang is on record even if nobody kills it.
- ``BackgroundEmbedder`` indexes new events off the write path. An embedding
  call can take seconds on a cold model, and it ran inside the single write
  slot, so one slow call held back every agent's write. Now a write commits
  without its vector and wakes this thread, which fills the gap, keeps the
  model warm and backs off while the backend is down.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
import faulthandler
import logging
import sys
import threading
import time
from typing import Any, TextIO

import anyio

_LOGGER = logging.getLogger("max_chronicle.liveness")

LOOP_TICK_S = 0.5
LOOP_LAG_WARN_S = 1.0
STALL_DUMP_S = 10.0
LAG_WINDOW_S = 300.0  # how far back max_lag_ms looks

EMBED_IDLE_S = 300.0  # a sweep without a wake-up, for events written elsewhere
EMBED_BACKOFF_S = (60.0, 600.0)  # first and longest wait while the backend is down
EMBED_BATCH = 100
EMBED_DEBOUNCE_S = 0.2  # let a burst of writes land before one sweep


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LoopMonitor:
    """Event-loop lag, plus a stack dump when the loop stops ticking."""

    def __init__(
        self,
        *,
        tick_s: float = LOOP_TICK_S,
        warn_s: float = LOOP_LAG_WARN_S,
        stall_s: float = STALL_DUMP_S,
        stream: TextIO | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.tick_s = tick_s
        self.warn_s = warn_s
        self.stall_s = stall_s
        self._stream = stream
        self._clock = clock
        self._lock = threading.Lock()
        self._last_tick = clock()
        self._last_lag = 0.0
        self._samples: deque[tuple[float, float]] = deque()
        self._stalls = 0
        self._stall_dumped = False

    async def run(self) -> None:
        """Tick forever on the loop it runs on; cancel it to stop."""
        while True:
            started = self._clock()
            await anyio.sleep(self.tick_s)
            self.tick(self._clock() - started - self.tick_s)

    def tick(self, lag: float) -> None:
        """Record one wake-up that came *lag* seconds late."""
        lag = max(0.0, lag)
        now = self._clock()
        with self._lock:
            resumed = self._stall_dumped
            self._last_tick = now
            self._last_lag = lag
            self._stall_dumped = False
            self._samples.append((now, lag))
            while self._samples and now - self._samples[0][0] > LAG_WINDOW_S:
                self._samples.popleft()
        if resumed:
            _LOGGER.warning("event loop resumed after a %.1f s stall", lag + self.tick_s)
        elif lag >= self.warn_s:
            _LOGGER.warning("event loop lagged %.1f s", lag)

    def check_stall(self) -> bool:
        """Dump all stacks once per stall; True when this call dumped them."""
        with self._lock:
            silent = self._clock() - self._last_tick
            if silent < self.stall_s or self._stall_dumped:
                return False
            self._stall_dumped = True
            self._stalls += 1
        stream = self._stream or sys.stderr
        _LOGGER.error("event loop silent for %.1f s; stacks of every thread follow", silent)
        try:
            stream.flush()
            faulthandler.dump_traceback(file=stream, all_threads=True)
            stream.flush()
        except (OSError, ValueError, AttributeError) as exc:  # a stream without a file descriptor
            _LOGGER.error("could not dump stacks: %s: %s", type(exc).__name__, exc)
        return True

    def start_watcher(self, interval_s: float = 1.0) -> threading.Thread:
        def watch() -> None:
            while True:
                time.sleep(interval_s)
                try:
                    self.check_stall()
                except Exception:  # noqa: BLE001 - the watcher must outlive any one check
                    _LOGGER.exception("stall check failed")

        thread = threading.Thread(target=watch, name="chronicle-loop-watcher", daemon=True)
        thread.start()
        return thread

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            peak = max((lag for _, lag in self._samples), default=0.0)
            return {
                "lag_ms": round(self._last_lag * 1000),
                "max_lag_ms_5m": round(peak * 1000),
                "last_tick_age_s": round(self._clock() - self._last_tick, 1),
                "stalls": self._stalls,
            }


class BackgroundEmbedder:
    """Fill the vector index for events that were written without one."""

    def __init__(
        self,
        manifest: Callable[[], dict[str, Any]],
        embed: Callable[..., dict[str, Any]],
        *,
        idle_s: float = EMBED_IDLE_S,
        backoff_s: tuple[float, float] = EMBED_BACKOFF_S,
        batch: int = EMBED_BATCH,
        debounce_s: float = EMBED_DEBOUNCE_S,
    ) -> None:
        self._manifest = manifest
        self._embed = embed
        self.idle_s = idle_s
        self.backoff_s = backoff_s
        self.batch = batch
        self.debounce_s = debounce_s
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._runs = 0
        self._last_run_at: str | None = None
        self._last_report: dict[str, Any] | None = None
        self._backoff: float | None = None

    def wake(self) -> None:
        self._wake.set()

    def run_once(self) -> dict[str, Any]:
        try:
            report = self._embed(self._manifest(), limit=self.batch)
        except Exception as exc:  # noqa: BLE001 - a failed sweep is retried, never fatal
            _LOGGER.exception("background embedding failed")
            report = {"error": f"{type(exc).__name__}: {exc}", "backend_unavailable": False}
        with self._lock:
            self._runs += 1
            self._last_run_at = _utc_stamp()
            self._last_report = {key: report.get(key) for key in (
                "embedded", "failed", "total_without_embedding", "backend_unavailable", "error")
                if key in report}
        if report.get("embedded"):
            _LOGGER.info("background embedding: %s", self._last_report)
        return report

    def next_wait(self, report: dict[str, Any]) -> float:
        """Seconds to sleep after *report*, unless a write wakes the thread first."""
        if report.get("backend_unavailable"):
            first, longest = self.backoff_s
            self._backoff = first if self._backoff is None else min(self._backoff * 2, longest)
            return self._backoff
        self._backoff = None
        remaining = (report.get("total_without_embedding") or 0) - (report.get("embedded") or 0)
        if report.get("embedded") and remaining > 0:
            return 0.0  # more of a backlog than one batch
        return self.idle_s

    def wait_for_work(self, wait: float) -> None:
        """Sleep *wait* seconds or until a write, which a backoff ignores."""
        deadline = time.monotonic() + wait
        while (remaining := deadline - time.monotonic()) > 0:
            if not self._wake.wait(remaining):
                break
            if self._backoff is None:
                time.sleep(self.debounce_s)
                break
            # The backend is down: a write must not retry it before the backoff ends.
            self._wake.clear()
        self._wake.clear()

    def loop(self) -> None:
        wait = 0.0  # the first sweep runs at once and warms the model
        while True:
            self.wait_for_work(wait)
            wait = self.next_wait(self.run_once())

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.loop, name="chronicle-embedder", daemon=True)
        thread.start()
        return thread

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"runs": self._runs, "last_run_at": self._last_run_at, "last_report": self._last_report}
