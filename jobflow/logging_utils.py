from __future__ import annotations

import logging
from typing import Any

INFO_VERBOSE_LEVEL = 15
INFO_VERBOSE_NAME = "INFO_VERBOSE"


def register_logging_levels() -> None:
    if logging.getLevelName(INFO_VERBOSE_LEVEL) != INFO_VERBOSE_NAME:
        logging.addLevelName(INFO_VERBOSE_LEVEL, INFO_VERBOSE_NAME)
    if getattr(logging, INFO_VERBOSE_NAME, None) != INFO_VERBOSE_LEVEL:
        setattr(logging, INFO_VERBOSE_NAME, INFO_VERBOSE_LEVEL)


def resolve_log_level(raw_level: Any, default: int = logging.INFO) -> int:
    register_logging_levels()
    if raw_level is None:
        return int(default)
    if isinstance(raw_level, int):
        return raw_level

    name = str(raw_level).strip().upper()
    if not name:
        return int(default)

    aliases = {
        "INFO_VERBOSE": INFO_VERBOSE_LEVEL,
        "VERBOSE_INFO": INFO_VERBOSE_LEVEL,
        "INFO_OLD": INFO_VERBOSE_LEVEL,
    }
    if name in aliases:
        return aliases[name]

    value = getattr(logging, name, None)
    if isinstance(value, int):
        return value

    try:
        return int(name)
    except ValueError:
        return int(default)
