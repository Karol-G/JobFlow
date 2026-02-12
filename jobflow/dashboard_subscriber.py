from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .dashboard import RICH_AVAILABLE, _format_duration, _stacked_bar
from .telemetry import TelemetrySnapshot, snapshot_from_dict

logger = logging.getLogger(__name__)

try:
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
except Exception:  # pragma: no cover - optional dependency
    pass


class SnapshotDashboard:
    def __init__(self, *, refresh_s: float) -> None:
        if not RICH_AVAILABLE:  # pragma: no cover - depends on optional dependency
            raise RuntimeError("rich is required for dashboard subscriber")
        self.refresh_s = max(0.1, float(refresh_s))
        self.console = Console()
        self._started_ts = time.time()
        self._live = Live(self._render(None, stale=True, source_path=None), console=self.console, screen=True, auto_refresh=False)
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._live.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._live.stop()
        self._started = False

    def update(self, snapshot: Optional[TelemetrySnapshot], *, stale: bool, source_path: Optional[Path]) -> None:
        if not self._started:
            return
        self._live.update(self._render(snapshot, stale=stale, source_path=source_path), refresh=True)

    def _render(self, snapshot: Optional[TelemetrySnapshot], *, stale: bool, source_path: Optional[Path]):
        task_counts = snapshot.task_counts if snapshot is not None else {}
        worker_counts = snapshot.worker_counts if snapshot is not None else {}
        launch_counts = snapshot.launch_counts if snapshot is not None else {}

        total_tasks = sum(task_counts.values())
        succeeded = task_counts.get("SUCCEEDED", 0)
        failed = task_counts.get("FAILED", 0)
        in_progress = task_counts.get("ASSIGNED", 0) + task_counts.get("RUNNING", 0)
        other = max(0, total_tasks - succeeded - failed - in_progress)

        bar_width = max(20, min(120, self.console.size.width - 20))
        bar = _stacked_bar(
            width=bar_width,
            values=[
                ("green", succeeded),
                ("red", failed),
                ("yellow", in_progress),
                ("grey50", other),
            ],
            total=max(1, total_tasks),
        )

        pct = (100.0 * (succeeded + failed) / total_tasks) if total_tasks > 0 else 0.0
        done = succeeded + failed
        elapsed_s = max(0.0, time.time() - self._started_ts)
        eta_s: Optional[float] = None
        if total_tasks > 0 and done > 0 and elapsed_s > 0:
            rate = done / elapsed_s
            if rate > 0:
                eta_s = max(0.0, (total_tasks - done) / rate)

        progress_line = Text(
            f"done={done}/{total_tasks} ({pct:.1f}%, {_format_duration(elapsed_s)}<{_format_duration(eta_s) if eta_s is not None else '?'}) "
            f"succeeded={succeeded} failed={failed} in_progress={in_progress} other={other}",
            style="bold",
        )
        if stale:
            progress_line.append("  [stale]", style="bold red")

        progress_lines = Group(bar, progress_line)

        worker_total = sum(worker_counts.values())
        worker_alive = worker_total - worker_counts.get("OFFLINE", 0)
        manager_pid = snapshot.manager_pid if snapshot is not None else 0
        pending_shutdown_acks = snapshot.pending_shutdown_acks if snapshot is not None else 0
        shutdown_started = snapshot.shutdown_started if snapshot is not None else False

        stats = Table.grid(expand=True, padding=(0, 2))
        stats.add_column(justify="left", ratio=1)
        stats.add_column(justify="left", ratio=1)
        stats.add_column(justify="left", ratio=1)
        stats.add_row(
            f"Workers total: {worker_total}",
            f"Workers alive: {worker_alive}",
            f"Workers offline: {worker_counts.get('OFFLINE', 0)}",
        )
        stats.add_row(
            f"Manager PID: {manager_pid if manager_pid > 0 else '?'}",
            f"Source: {source_path if source_path is not None else '?'}",
            "",
        )
        stats.add_row(
            f"Idle: {worker_counts.get('IDLE', 0)}",
            f"Busy: {worker_counts.get('BUSY', 0)}",
            f"Pending(starting): {worker_counts.get('STARTING', 0)}",
        )
        stats.add_row(
            f"Draining: {worker_counts.get('DRAINING', 0)}",
            f"Launch pending: {launch_counts.get('PENDING', 0)}",
            f"Launch running: {launch_counts.get('RUNNING', 0)}",
        )
        stats.add_row(
            f"Launch online: {launch_counts.get('ONLINE', 0)}",
            f"Launch failed: {launch_counts.get('FAILED', 0)}",
            f"Launch stale: {launch_counts.get('STALE', 0)}",
        )
        if shutdown_started:
            stats.add_row(
                "Shutdown: in progress",
                f"Pending ShutdownAck: {pending_shutdown_acks}",
                "",
            )

        if snapshot is None or not snapshot.recent_logs:
            log_text = Text("(manager logs not published)", style="dim")
        else:
            max_log_lines = max(6, self.console.size.height - 16)
            lines = list(snapshot.recent_logs)[-max_log_lines:]
            log_text = Text("\n".join(lines))

        layout = Layout()
        layout.split_column(
            Layout(Panel(progress_lines, title="Task Progress (External)", border_style="cyan"), size=4),
            Layout(Panel(stats, title="Manager Stats", border_style="cyan"), size=7),
            Layout(Rule(style="grey50"), size=1),
            Layout(Panel(log_text, title="Manager Log", border_style="cyan"), ratio=1),
        )
        return layout


def _load_snapshot_if_changed(path: Path, last_mtime_ns: Optional[int]) -> tuple[Optional[TelemetrySnapshot], Optional[int]]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None, last_mtime_ns

    mtime_ns = stat.st_mtime_ns
    if last_mtime_ns is not None and mtime_ns == last_mtime_ns:
        return None, last_mtime_ns

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed reading telemetry snapshot: %s", path)
        return None, last_mtime_ns

    if not isinstance(raw, dict):
        logger.warning("Telemetry snapshot root is not an object: %s", path)
        return None, mtime_ns

    try:
        snapshot = snapshot_from_dict(raw)
    except Exception:
        logger.exception("Failed parsing telemetry snapshot object: %s", path)
        return None, mtime_ns
    return snapshot, mtime_ns


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JobFlow external dashboard subscriber")
    parser.add_argument("--telemetry-file", required=True)
    parser.add_argument("--refresh", type=float, default=0.5)
    parser.add_argument("--stale-timeout", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if not RICH_AVAILABLE:
        raise RuntimeError("Dashboard subscriber requires 'rich'. Install with: pip install .[dashboard]")

    telemetry_file = Path(args.telemetry_file).expanduser().resolve()
    refresh_s = max(0.1, float(args.refresh))
    stale_timeout_s = max(refresh_s, float(args.stale_timeout))

    running = True

    def _stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    dashboard = SnapshotDashboard(refresh_s=refresh_s)
    last_mtime_ns: Optional[int] = None
    last_snapshot: Optional[TelemetrySnapshot] = None
    last_snapshot_seen_ts: Optional[float] = None

    try:
        dashboard.start()
        while running:
            snapshot, last_mtime_ns = _load_snapshot_if_changed(telemetry_file, last_mtime_ns)
            if snapshot is not None:
                last_snapshot = snapshot
                last_snapshot_seen_ts = time.time()

            now = time.time()
            stale = last_snapshot_seen_ts is None or (now - last_snapshot_seen_ts > stale_timeout_s)
            dashboard.update(last_snapshot, stale=stale, source_path=telemetry_file)
            time.sleep(refresh_s)
    finally:
        dashboard.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
