from __future__ import annotations

from abc import ABC, abstractmethod
import hashlib
import importlib.util
import sys
from importlib import import_module
from pathlib import Path
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


def _load_module_from_file(module_path: Path):
    resolved = module_path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Program file not found: {resolved}")

    module_key = f"jobflow_user_program_{hashlib.sha1(str(resolved).encode('utf-8')).hexdigest()}"
    existing = sys.modules.get(module_key)
    if existing is not None:
        return existing

    spec = importlib.util.spec_from_file_location(module_key, str(resolved))
    if spec is None or spec.loader is None:
        raise ValueError(f"Failed to load module from file: {resolved}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)
    return module


def _resolve_program_class(module, class_name: str, source: str):
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ValueError(f"Class '{class_name}' not found in '{source}'.")
    if not isinstance(cls, type) or not issubclass(cls, TaskProgram):
        raise TypeError(f"{source}:{class_name} is not a TaskProgram subclass.")
    return cls


def _single_task_program_class(module, source: str):
    candidates = [
        obj for obj in module.__dict__.values() if isinstance(obj, type) and issubclass(obj, TaskProgram) and obj is not TaskProgram
    ]
    if not candidates:
        raise ValueError(f"No TaskProgram subclass found in '{source}'.")
    if len(candidates) > 1:
        names = ", ".join(sorted(cls.__name__ for cls in candidates))
        raise ValueError(f"Multiple TaskProgram classes found in '{source}': {names}. Use '<source>:ClassName'.")
    return candidates[0]


def load_program(qualname: str, program_args: Optional[dict] = None) -> TaskProgram:
    if ":" in qualname:
        module_or_path, class_name = qualname.split(":", 1)
        if not class_name:
            raise ValueError("Program class name cannot be empty after ':'.")
        if module_or_path.endswith(".py"):
            module = _load_module_from_file(Path(module_or_path))
            cls = _resolve_program_class(module, class_name, str(Path(module_or_path).expanduser().resolve()))
        else:
            module = import_module(module_or_path)
            cls = _resolve_program_class(module, class_name, module_or_path)
    else:
        if not qualname.endswith(".py"):
            raise ValueError("Program must be 'module.submodule:ClassName' or '/path/to/file.py[:ClassName]'.")
        module = _load_module_from_file(Path(qualname))
        cls = _single_task_program_class(module, str(Path(qualname).expanduser().resolve()))

    return cls(program_args=program_args or {})
