"""The server watches its own event loop and indexes events off the write path."""

from __future__ import annotations

import logging
import threading

import anyio

from max_chronicle.liveness import BackgroundEmbedder, LoopMonitor


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_a_stalled_loop_dumps_every_threads_stack_once(tmp_path, caplog) -> None:
    clock = _Clock()
    dump = (tmp_path / "stacks.txt").open("w+")
    monitor = LoopMonitor(stall_s=10.0, stream=dump, clock=clock)

    clock.now += 11.0
    with caplog.at_level(logging.ERROR, logger="max_chronicle.liveness"):
        first = monitor.check_stall()
        again = monitor.check_stall()

    dump.seek(0)
    stacks = dump.read()
    assert (first, again) == (True, False)
    assert "thread" in stacks.lower() and "test_a_stalled_loop_dumps_every_threads_stack_once" in stacks
    assert "silent for 11.0 s" in caplog.text
    assert monitor.snapshot()["stalls"] == 1


def test_a_loop_that_ticks_again_can_be_dumped_on_its_next_stall(tmp_path) -> None:
    clock = _Clock()
    monitor = LoopMonitor(stall_s=10.0, stream=(tmp_path / "s.txt").open("w"), clock=clock)
    clock.now += 11.0
    monitor.check_stall()

    monitor.tick(0.0)  # the loop came back
    clock.now += 11.0

    assert monitor.check_stall() is True
    assert monitor.snapshot()["stalls"] == 2


def test_a_ticking_loop_is_not_dumped(tmp_path) -> None:
    clock = _Clock()
    monitor = LoopMonitor(stall_s=10.0, stream=(tmp_path / "s.txt").open("w"), clock=clock)
    for _ in range(30):
        clock.now += 0.5
        monitor.tick(0.0)

    assert monitor.check_stall() is False


def test_lag_over_the_threshold_is_logged_and_reported(caplog) -> None:
    clock = _Clock()
    monitor = LoopMonitor(warn_s=1.0, clock=clock)

    with caplog.at_level(logging.WARNING, logger="max_chronicle.liveness"):
        monitor.tick(0.2)
        monitor.tick(2.5)
        monitor.tick(0.1)

    assert "lagged 2.5 s" in caplog.text and "lagged 0.2" not in caplog.text
    snapshot = monitor.snapshot()
    assert (snapshot["lag_ms"], snapshot["max_lag_ms_5m"]) == (100, 2500)


def test_the_monitor_measures_a_real_blocked_loop() -> None:
    """A synchronous sleep on the loop shows up as lag on the next tick."""
    import time

    monitor = LoopMonitor(tick_s=0.05)

    async def main() -> None:
        async with anyio.create_task_group() as group:
            group.start_soon(monitor.run)
            await anyio.sleep(0.12)
            time.sleep(0.4)  # blocks the loop, as a stuck handler would
            await anyio.sleep(0.12)
            group.cancel_scope.cancel()

    anyio.run(main)

    assert monitor.snapshot()["max_lag_ms_5m"] >= 300


def _embedder(reports: list[dict], **kwargs) -> tuple[BackgroundEmbedder, list[int | None]]:
    calls: list[int | None] = []

    def embed(manifest, *, limit=None):
        calls.append(limit)
        return reports.pop(0) if reports else {"embedded": 0, "total_without_embedding": 0}

    return BackgroundEmbedder(lambda: {}, embed, **kwargs), calls


def test_embedder_backs_off_while_the_backend_is_down_and_resets_after() -> None:
    embedder, _ = _embedder([], backoff_s=(60.0, 600.0), idle_s=300.0)
    down = {"backend_unavailable": True, "embedded": 0}

    waits = [embedder.next_wait(down) for _ in range(5)]
    waits.append(embedder.next_wait({"embedded": 1, "total_without_embedding": 1}))

    assert waits == [60.0, 120.0, 240.0, 480.0, 600.0, 300.0]


def test_embedder_keeps_going_while_a_backlog_remains() -> None:
    embedder, _ = _embedder([])

    assert embedder.next_wait({"embedded": 100, "total_without_embedding": 250}) == 0.0
    assert embedder.next_wait({"embedded": 0, "total_without_embedding": 3}) == embedder.idle_s


def test_a_write_wakes_the_embedder_but_not_during_a_backoff() -> None:
    embedder, _ = _embedder([], debounce_s=0.0)
    woke = threading.Event()

    def wait_then_flag(seconds):
        embedder.wait_for_work(seconds)
        woke.set()

    worker = threading.Thread(target=wait_then_flag, args=(30.0,), daemon=True)
    worker.start()
    embedder.wake()
    assert woke.wait(2.0), "a write should end the idle wait"

    embedder.next_wait({"backend_unavailable": True})  # now backing off
    woke.clear()
    worker = threading.Thread(target=wait_then_flag, args=(0.5,), daemon=True)
    worker.start()
    embedder.wake()
    assert not woke.wait(0.2), "a write must not cut a backoff short"
    assert woke.wait(2.0)


def test_a_failing_sweep_is_reported_not_raised(caplog) -> None:
    def explode(manifest, *, limit=None):
        raise RuntimeError("ollama exploded")

    embedder = BackgroundEmbedder(lambda: {}, explode)

    report = embedder.run_once()

    assert "RuntimeError" in report["error"]
    assert embedder.snapshot()["runs"] == 1


def test_a_blocked_loop_is_dumped_by_the_c_timer_even_while_the_gil_is_held(tmp_path) -> None:
    """The Python watcher needs the GIL; faulthandler's timer thread does not."""
    import time

    dump = (tmp_path / "stacks.txt").open("w+")
    monitor = LoopMonitor(tick_s=0.05, stall_s=0.3, stream=dump)

    async def main() -> None:
        async with anyio.create_task_group() as group:
            group.start_soon(monitor.run)
            await anyio.sleep(0.1)
            time.sleep(0.8)  # the loop is stuck well past stall_s
            await anyio.sleep(0.1)
            group.cancel_scope.cancel()

    anyio.run(main)

    dump.seek(0)
    assert "test_a_blocked_loop_is_dumped_by_the_c_timer" in dump.read()
