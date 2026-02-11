from __future__ import annotations

from abc import ABC, abstractmethod
from importlib import import_module
from typing import Callable, Iterable, Optional

from .models import TaskDefinition

ProgressCallback = Callable[[float, Optional[dict]], None]


class TaskProgram(ABC):
    def __init__(self, *, program_args: Optional[dict] = None) -> None:
        self.program_args = program_args or {}

    @classmethod
    def name(cls) -> str:
        return cls.__name__

    @abstractmethod
    def generate_tasks(self) -> Iterable[TaskDefinition]:
        raise NotImplementedError

    @abstractmethod
    def execute_task(self, spec: dict, progress_cb: ProgressCallback) -> dict:
        raise NotImplementedError


def load_program(qualname: str, program_args: Optional[dict] = None) -> TaskProgram:
    if ":" not in qualname:
        raise ValueError("Program qualname must be 'module.submodule:ClassName'.")

    module_name, class_name = qualname.split(":", 1)
    module = import_module(module_name)
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ValueError(f"Class '{class_name}' not found in module '{module_name}'.")
    if not isinstance(cls, type) or not issubclass(cls, TaskProgram):
        raise TypeError(f"{qualname} is not a TaskProgram subclass.")

    return cls(program_args=program_args or {})
