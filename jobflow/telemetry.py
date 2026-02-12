from __future__ import annotations

import json
import logging
import queue
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class TelemetrySnapshot:
    ts: float
    manager_pid: int
    task_counts: dict[str, int]
    worker_counts: dict[str, int]
    launch_counts: dict[str, int]
    shutdown_started: bool
    pending_shutdown_acks: int
    recent_logs: tuple[str, ...] = ()


class TelemetryPublisher(ABC):
    @abstractmethod
    def publish(self, snapshot: TelemetrySnapshot) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class NoopTelemetryPublisher(TelemetryPublisher):
    def publish(self, snapshot: TelemetrySnapshot) -> None:
        _ = snapshot

    def close(self) -> None:
        return


def snapshot_to_dict(snapshot: TelemetrySnapshot) -> dict[str, Any]:
    return {
        "ts": float(snapshot.ts),
        "manager_pid": int(snapshot.manager_pid),
        "task_counts": dict(snapshot.task_counts),
        "worker_counts": dict(snapshot.worker_counts),
        "launch_counts": dict(snapshot.launch_counts),
        "shutdown_started": bool(snapshot.shutdown_started),
        "pending_shutdown_acks": int(snapshot.pending_shutdown_acks),
        "recent_logs": list(snapshot.recent_logs),
    }


def snapshot_from_dict(raw: dict[str, Any]) -> TelemetrySnapshot:
    return TelemetrySnapshot(
        ts=float(raw.get("ts", 0.0)),
        manager_pid=int(raw.get("manager_pid", 0)),
        task_counts=_coerce_count_map(raw.get("task_counts")),
        worker_counts=_coerce_count_map(raw.get("worker_counts")),
        launch_counts=_coerce_count_map(raw.get("launch_counts")),
        shutdown_started=bool(raw.get("shutdown_started", False)),
        pending_shutdown_acks=int(raw.get("pending_shutdown_acks", 0)),
        recent_logs=tuple(_coerce_str_list(raw.get("recent_logs"))),
    )


class FileTelemetryPublisher(TelemetryPublisher):
    """
    Asynchronous telemetry publisher that writes only the latest snapshot to a JSON file.

    publish() is non-blocking:
    - bounded queue
    - drop-oldest policy when full
    """

    def __init__(self, path: Path, *, max_queue: int = 2) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_queue = max(1, int(max_queue))
        self._queue: queue.Queue[TelemetrySnapshot | None] = queue.Queue(maxsize=self.max_queue)
        self._closed = False
        self._failed = False
        self._thread = threading.Thread(target=self._writer_loop, name="jobflow-telemetry-writer", daemon=True)
        self._thread.start()

    def publish(self, snapshot: TelemetrySnapshot) -> None:
        if self._closed or self._failed:
            return
        try:
            self._queue.put_nowait(snapshot)
            return
        except queue.Full:
            pass

        # Drop the oldest snapshot to keep manager-side publishing non-blocking.
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass

        try:
            self._queue.put_nowait(snapshot)
        except queue.Full:
            # If still full due to a race, drop this publish attempt.
            pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
        self._thread.join(timeout=2.0)

    def _writer_loop(self) -> None:
        try:
            while True:
                item = self._queue.get()
                if item is None:
                    break

                latest = item
                should_stop = False
                while True:
                    try:
                        next_item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if next_item is None:
                        should_stop = True
                        break
                    latest = next_item

                self._write_snapshot(latest)
                if should_stop:
                    break
        except Exception:
            self._failed = True
            logger.exception("Telemetry file publisher failed; file publishing disabled.")

    def _write_snapshot(self, snapshot: TelemetrySnapshot) -> None:
        data = snapshot_to_dict(snapshot)
        payload = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        tmp = self.path.with_suffix(self.path.suffix + f".tmp.{uuid.uuid4().hex}")
        tmp.write_bytes(payload)
        tmp.replace(self.path)


def _coerce_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, int] = {}
    for key, raw_val in value.items():
        if not isinstance(key, str):
            continue
        try:
            out[key] = int(raw_val)
        except (TypeError, ValueError):
            continue
    return out


def _coerce_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str):
            out.append(item)
    return out
