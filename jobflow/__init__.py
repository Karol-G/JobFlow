"""JobFlow is a lightweight, programmable system for generating, distributing, and executing jobs on batch-scheduled clusters.."""

from importlib import metadata as _metadata

__all__ = ["__version__"]

try:
    __version__ = _metadata.version(__name__)
except _metadata.PackageNotFoundError:  # pragma: no cover - during editable installs pre-build
    __version__ = "0.0.0"
