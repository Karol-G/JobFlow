import logging

from jobflow.logging_utils import INFO_VERBOSE_LEVEL, INFO_VERBOSE_NAME, register_logging_levels, resolve_log_level


def test_register_logging_levels_adds_info_verbose() -> None:
    register_logging_levels()
    assert logging.getLevelName(INFO_VERBOSE_LEVEL) == INFO_VERBOSE_NAME


def test_resolve_log_level_supports_info_verbose_aliases() -> None:
    assert resolve_log_level("INFO_VERBOSE") == INFO_VERBOSE_LEVEL
    assert resolve_log_level("info_old") == INFO_VERBOSE_LEVEL
    assert resolve_log_level("VERBOSE_INFO") == INFO_VERBOSE_LEVEL


def test_resolve_log_level_keeps_standard_info() -> None:
    assert resolve_log_level("INFO") == logging.INFO
