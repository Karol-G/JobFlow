from __future__ import annotations

from abc import ABC, abstractmethod


class Transport(ABC):
    @abstractmethod
    def send(self, dst_id: str, msg: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout_s: float) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def mode(self) -> str:
        raise NotImplementedError
