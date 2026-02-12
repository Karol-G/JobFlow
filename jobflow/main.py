from __future__ import annotations

import argparse
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .manager import build_parser as build_manager_parser

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="JobFlow supervisor that can launch manager and optional external dashboard.",
    )
    parser.add_argument(
        "--external-dashboard",
        choices=["auto", "on", "off"],
        default="auto",
        help="Run dashboard in a separate subscriber process.",
    )
    parser.add_argument(
        "--external-dashboard-refresh",
        type=float,
        default=0.5,
        help="External dashboard refresh interval in seconds.",
    )
    parser.add_argument(
        "--external-dashboard-stale-timeout",
        type=float,
        default=5.0,
        help="Mark dashboard stale when snapshot age exceeds this many seconds.",
    )
    parser.add_argument(
        "--supervisor-log-level",
        default="INFO",
        help="Supervisor log level.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    wrapper_args, manager_argv = parser.parse_known_args(argv)

    logging.basicConfig(
        level=getattr(logging, wrapper_args.supervisor_log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    manager_parser = build_manager_parser()
    manager_args = manager_parser.parse_args(manager_argv)

    use_external_dashboard = _resolve_external_dashboard_mode(wrapper_args.external_dashboard)
    telemetry_file = _resolve_telemetry_file(manager_args)

    manager_cmd = _build_manager_cmd(manager_argv, use_external_dashboard, telemetry_file)
    logger.info("Starting manager process")
    manager_proc = subprocess.Popen(manager_cmd)

    dashboard_proc: Optional[subprocess.Popen] = None
    if use_external_dashboard:
        dashboard_cmd = _build_dashboard_cmd(
            telemetry_file=telemetry_file,
            refresh_s=float(wrapper_args.external_dashboard_refresh),
            stale_timeout_s=float(wrapper_args.external_dashboard_stale_timeout),
        )
        try:
            logger.info("Starting external dashboard process")
            dashboard_proc = subprocess.Popen(dashboard_cmd)
        except Exception:
            logger.exception("Failed to start dashboard subscriber. Manager will continue without it.")
            dashboard_proc = None

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            rc = manager_proc.poll()
            if rc is not None:
                logger.info("Manager exited with code %s", rc)
                _terminate_process(dashboard_proc, name="dashboard")
                return int(rc)

            if dashboard_proc is not None:
                dash_rc = dashboard_proc.poll()
                if dash_rc is not None:
                    logger.warning("Dashboard exited with code %s; manager continues.", dash_rc)
                    dashboard_proc = None

            time.sleep(0.2)

        logger.info("Shutdown requested; stopping child processes.")
        _terminate_process(manager_proc, name="manager")
        _terminate_process(dashboard_proc, name="dashboard")
        return 0
    finally:
        _terminate_process(manager_proc, name="manager")
        _terminate_process(dashboard_proc, name="dashboard")


def _resolve_external_dashboard_mode(mode: str) -> bool:
    if mode == "off":
        return False
    if mode == "on":
        return True
    return bool(sys.stdout.isatty() and sys.stderr.isatty())


def _resolve_telemetry_file(manager_args: argparse.Namespace) -> str:
    if manager_args.telemetry_file:
        return str(manager_args.telemetry_file)
    if manager_args.mode == "fs":
        shared_dir = manager_args.shared_dir or str(Path.cwd())
        session_id = manager_args.session_id or "jobflow-session"
        return str(Path(shared_dir) / session_id / "dashboard_snapshot.json")
    return str(Path(manager_args.db_path).with_suffix(".dashboard.json"))


def _build_manager_cmd(manager_argv: list[str], use_external_dashboard: bool, telemetry_file: str) -> list[str]:
    cmd = [sys.executable, "-m", "jobflow.manager", *manager_argv]
    if use_external_dashboard:
        # Keep manager control-plane independent from UI rendering.
        cmd.extend(
            [
                "--dashboard",
                "off",
                "--telemetry-mode",
                "file",
                "--telemetry-file",
                telemetry_file,
            ]
        )
    return cmd


def _build_dashboard_cmd(*, telemetry_file: str, refresh_s: float, stale_timeout_s: float) -> list[str]:
    return [
        sys.executable,
        "-m",
        "jobflow.dashboard_subscriber",
        "--telemetry-file",
        telemetry_file,
        "--refresh",
        str(refresh_s),
        "--stale-timeout",
        str(stale_timeout_s),
    ]


def _terminate_process(proc: Optional[subprocess.Popen], *, name: str) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5.0)
    except Exception:
        logger.warning("Force killing %s process", name)
        try:
            proc.kill()
            proc.wait(timeout=2.0)
        except Exception:
            logger.exception("Failed stopping %s process", name)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
