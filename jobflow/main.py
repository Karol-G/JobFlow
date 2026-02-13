from __future__ import annotations

import argparse
import logging
import select
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .logging_utils import resolve_log_level
from .manager import build_parser as build_manager_parser

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="JobFlow supervisor that can launch manager and optional external dashboard.",
    )
    parser.add_argument(
        "--external-dashboard",
        choices=["auto", "on", "off"],
        default="on",
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
        level=resolve_log_level(wrapper_args.supervisor_log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    manager_parser = build_manager_parser()
    manager_args = manager_parser.parse_args(manager_argv)

    use_external_dashboard = _resolve_external_dashboard_mode(wrapper_args.external_dashboard)
    telemetry_file = _resolve_telemetry_file(manager_args)

    manager_cmd = _build_manager_cmd(manager_argv, use_external_dashboard, telemetry_file)
    logger.info("Starting manager process")
    manager_proc = subprocess.Popen(manager_cmd)
    manager_shutdown_wait_s = max(5.0, float(manager_args.shutdown_grace_period) + 5.0)

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
        with _SingleKeyReader() as key_reader:
            if key_reader.is_enabled:
                logger.info("Press 'q' to request graceful shutdown.")
            while running:
                key = key_reader.poll_key()
                if key is not None and key.lower() == "q":
                    logger.info("Received 'q'; requesting graceful shutdown.")
                    running = False
                    break

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
        _terminate_process(manager_proc, name="manager", timeout_s=manager_shutdown_wait_s)
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


def _terminate_process(proc: Optional[subprocess.Popen], *, name: str, timeout_s: float = 5.0) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=max(0.1, float(timeout_s)))
    except Exception:
        logger.warning("Force killing %s process", name)
        try:
            proc.kill()
            proc.wait(timeout=2.0)
        except Exception:
            logger.exception("Failed stopping %s process", name)


class _SingleKeyReader:
    def __init__(self) -> None:
        self.is_enabled = False
        self._fd: Optional[int] = None
        self._old_termios = None

    def __enter__(self) -> "_SingleKeyReader":
        if not sys.stdin.isatty():
            return self

        try:
            import termios
            import tty
        except Exception:
            return self

        try:
            self._fd = sys.stdin.fileno()
            self._old_termios = termios.tcgetattr(self._fd)
            tty.setcbreak(self._fd)
            self.is_enabled = True
        except Exception:
            self._fd = None
            self._old_termios = None
            self.is_enabled = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.is_enabled or self._fd is None or self._old_termios is None:
            return
        try:
            import termios

            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_termios)
        except Exception:
            logger.exception("Failed restoring terminal mode.")

    def poll_key(self) -> Optional[str]:
        if not self.is_enabled or self._fd is None:
            return None
        try:
            ready, _, _ = select.select([self._fd], [], [], 0)
            if not ready:
                return None
            return sys.stdin.read(1)
        except Exception:
            return None


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
