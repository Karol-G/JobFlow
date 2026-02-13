from __future__ import annotations

import itertools
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Any, Optional

from .base import Launcher

logger = logging.getLogger(__name__)


PRIORITY_CANCEL = 0
PRIORITY_SUBMIT = 1
PRIORITY_POLL = 2
PRIORITY_STOP = 99


@dataclass(slots=True)
class LauncherCommand:
    command_id: int
    kind: str
    payload: dict[str, Any]
    timeout_s: float
    max_retries: int


@dataclass(slots=True)
class LauncherResult:
    command_id: int
    kind: str
    ok: bool
    payload: dict[str, Any]
    attempts: int
    timed_out: bool = False
    error: Optional[str] = None


class LauncherService:
    """
    Background actor that performs launcher I/O away from the manager hot loop.

    The manager submits commands and drains results. All DB/state writes stay on the
    manager thread.
    """

    def __init__(
        self,
        launcher: Launcher,
        *,
        command_queue_size: int = 256,
        result_queue_size: int = 512,
    ) -> None:
        self.launcher = launcher
        self._commands: queue.PriorityQueue[tuple[int, int, LauncherCommand]] = queue.PriorityQueue(maxsize=max(1, int(command_queue_size)))
        self._results: queue.Queue[LauncherResult] = queue.Queue(maxsize=max(1, int(result_queue_size)))
        self._id_counter = itertools.count(1)
        self._seq_counter = itertools.count(1)
        self._stop_requested = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._thread = threading.Thread(target=self._run, name="jobflow-launcher-service", daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop_requested.set()
        self._enqueue_stop()
        if self._thread is not None:
            self._thread.join(timeout=max(0.1, float(timeout_s)))

    def enqueue(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        priority: int,
        timeout_s: float,
        max_retries: int,
    ) -> Optional[int]:
        command_id = next(self._id_counter)
        cmd = LauncherCommand(
            command_id=command_id,
            kind=kind,
            payload=payload,
            timeout_s=max(0.1, float(timeout_s)),
            max_retries=max(0, int(max_retries)),
        )
        item = (int(priority), next(self._seq_counter), cmd)
        try:
            self._commands.put_nowait(item)
        except queue.Full:
            logger.warning("LauncherService command queue full; dropping command kind=%s", kind)
            return None
        return command_id

    def drain_results(self, *, max_items: int = 256) -> list[LauncherResult]:
        out: list[LauncherResult] = []
        limit = max(1, int(max_items))
        for _ in range(limit):
            try:
                out.append(self._results.get_nowait())
            except queue.Empty:
                break
        return out

    def _enqueue_stop(self) -> None:
        cmd = LauncherCommand(command_id=0, kind="stop", payload={}, timeout_s=0.1, max_retries=0)
        item = (PRIORITY_STOP, next(self._seq_counter), cmd)
        try:
            self._commands.put_nowait(item)
        except queue.Full:
            # If the queue is full, opportunistically clear one old poll command.
            self._drop_one_pending_poll_command()
            try:
                self._commands.put_nowait(item)
            except queue.Full:
                logger.warning("LauncherService could not enqueue stop command; thread may take longer to stop.")

    def _drop_one_pending_poll_command(self) -> None:
        buffered: list[tuple[int, int, LauncherCommand]] = []
        dropped = False
        while True:
            try:
                item = self._commands.get_nowait()
            except queue.Empty:
                break
            if (not dropped) and item[2].kind == "poll":
                dropped = True
                continue
            buffered.append(item)
        for item in buffered:
            try:
                self._commands.put_nowait(item)
            except queue.Full:
                break

    def _run(self) -> None:
        while True:
            if self._stop_requested.is_set() and self._commands.empty():
                break
            try:
                _, _, cmd = self._commands.get(timeout=0.2)
            except queue.Empty:
                continue

            if cmd.kind == "stop":
                if self._stop_requested.is_set():
                    break
                continue

            result = self._execute_with_retries(cmd)
            self._publish_result(result)

    def _execute_with_retries(self, cmd: LauncherCommand) -> LauncherResult:
        max_attempts = 1 + max(0, int(cmd.max_retries))
        last_error = "unknown_error"
        last_timeout = False
        for attempt in range(1, max_attempts + 1):
            ok, payload, error, timed_out = self._execute_once(cmd)
            if ok:
                return LauncherResult(
                    command_id=cmd.command_id,
                    kind=cmd.kind,
                    ok=True,
                    payload=payload if isinstance(payload, dict) else {},
                    attempts=attempt,
                )
            last_error = str(error)
            last_timeout = bool(timed_out)
            if attempt < max_attempts:
                logger.warning(
                    "LauncherService command retry kind=%s attempt=%s/%s error=%s",
                    cmd.kind,
                    attempt,
                    max_attempts,
                    last_error,
                )
        return LauncherResult(
            command_id=cmd.command_id,
            kind=cmd.kind,
            ok=False,
            payload={},
            attempts=max_attempts,
            timed_out=last_timeout,
            error=last_error,
        )

    def _execute_once(self, cmd: LauncherCommand) -> tuple[bool, dict[str, Any], Optional[str], bool]:
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="jobflow-launcher-call")
        future = executor.submit(self._dispatch, cmd)
        try:
            result = future.result(timeout=max(0.1, float(cmd.timeout_s)))
            if not isinstance(result, dict):
                return True, {}, None, False
            return True, result, None, False
        except FutureTimeoutError:
            return False, {}, f"timeout after {cmd.timeout_s:.1f}s", True
        except Exception as exc:
            return False, {}, f"{type(exc).__name__}: {exc}", False
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _dispatch(self, cmd: LauncherCommand) -> dict[str, Any]:
        if cmd.kind == "submit":
            count = int(cmd.payload["count"])
            worker_command = list(cmd.payload["worker_command"])
            env = dict(cmd.payload.get("env", {}))
            requested = dict(cmd.payload.get("requested", {}))
            records = self.launcher.submit_workers(count, worker_command, env, requested)
            out: list[dict[str, Any]] = []
            for rec in records:
                out.append(
                    {
                        "launch_id": rec.launch_id,
                        "system": rec.system,
                        "batch_job_id": rec.batch_job_id,
                        "submitted_ts": rec.submitted_ts,
                        "last_update_ts": rec.last_update_ts,
                        "state": rec.state.value,
                        "worker_id_expected": rec.worker_id_expected,
                    }
                )
            return {"records": out}

        if cmd.kind == "poll":
            batch_job_ids = [str(i) for i in cmd.payload.get("batch_job_ids", []) if i]
            status_by_job = self.launcher.poll(batch_job_ids)
            return {"status_by_job": status_by_job}

        if cmd.kind == "cancel":
            batch_job_id = str(cmd.payload["batch_job_id"])
            self.launcher.cancel(batch_job_id)
            return {"batch_job_id": batch_job_id}

        raise ValueError(f"Unsupported launcher command kind: {cmd.kind}")

    def _publish_result(self, result: LauncherResult) -> None:
        if result.kind != "poll":
            while not self._stop_requested.is_set():
                try:
                    self._results.put(result, timeout=0.2)
                    return
                except queue.Full:
                    continue
            try:
                self._results.put_nowait(result)
                return
            except queue.Full:
                logger.warning("LauncherService result queue full; dropping non-poll result kind=%s command_id=%s", result.kind, result.command_id)
                return

        try:
            self._results.put(result, timeout=0.2)
            return
        except queue.Full:
            pass

        if result.kind == "poll":
            # Poll results are replaceable. Drop one stale poll result and retry.
            buffered: list[LauncherResult] = []
            dropped = False
            while True:
                try:
                    item = self._results.get_nowait()
                except queue.Empty:
                    break
                if (not dropped) and item.kind == "poll":
                    dropped = True
                    continue
                buffered.append(item)
            for item in buffered:
                try:
                    self._results.put_nowait(item)
                except queue.Full:
                    break
            try:
                self._results.put_nowait(result)
                return
            except queue.Full:
                pass

        logger.warning("LauncherService result queue full; dropping result kind=%s command_id=%s", result.kind, result.command_id)
