import argparse
from pathlib import Path

from jobflow.main import (
    _build_manager_cmd,
    _resolve_external_dashboard_mode,
    _resolve_telemetry_file,
)


def test_resolve_telemetry_file_fs_defaults_to_shared_session() -> None:
    args = argparse.Namespace(
        telemetry_file=None,
        mode="fs",
        shared_dir="/tmp/jobflow_shared",
        session_id="demo1",
        db_path="/tmp/unused.db",
    )
    out = _resolve_telemetry_file(args)
    assert out == "/tmp/jobflow_shared/demo1/dashboard_snapshot.json"


def test_resolve_telemetry_file_zmq_defaults_to_db_suffix() -> None:
    args = argparse.Namespace(
        telemetry_file=None,
        mode="zmq",
        shared_dir=None,
        session_id=None,
        db_path="/tmp/manager.db",
    )
    out = _resolve_telemetry_file(args)
    assert out == str(Path("/tmp/manager.db").with_suffix(".dashboard.json"))


def test_build_manager_cmd_appends_external_dashboard_overrides() -> None:
    cmd = _build_manager_cmd(
        manager_argv=["--mode", "fs", "--program", "x:y"],
        use_external_dashboard=True,
        telemetry_file="/tmp/snapshot.json",
    )
    assert cmd[1:3] == ["-m", "jobflow.manager"]
    assert "--dashboard" not in cmd
    assert "--telemetry-mode" in cmd
    assert "--telemetry-file" in cmd
    assert cmd[-1] == "/tmp/snapshot.json"


def test_build_manager_cmd_no_extra_overrides_when_external_off() -> None:
    cmd = _build_manager_cmd(
        manager_argv=["--mode", "fs", "--program", "x:y"],
        use_external_dashboard=False,
        telemetry_file="/tmp/snapshot.json",
    )
    assert "--telemetry-file" not in cmd
    assert "--telemetry-mode" not in cmd


def test_external_dashboard_mode_explicit_values() -> None:
    assert _resolve_external_dashboard_mode("off") is False
    assert _resolve_external_dashboard_mode("on") is True
