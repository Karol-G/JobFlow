from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import LaunchRecord


class Launcher(ABC):
    @abstractmethod
    def submit_workers(
        self,
        count: int,
        worker_command: list[str],
        env: dict[str, str],
        requested: dict,
    ) -> list[LaunchRecord]:
        raise NotImplementedError

    @abstractmethod
    def cancel(self, batch_job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, batch_job_ids: list[str]) -> dict[str, dict]:
        raise NotImplementedError
