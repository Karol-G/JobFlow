from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


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
