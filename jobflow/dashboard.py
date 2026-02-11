from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from typing import Optional

try:
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text

    RICH_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    RICH_AVAILABLE = False


class _RingLogHandler(logging.Handler):
    def __init__(self, *, max_lines: int) -> None:
        super().__init__()
        self._lines: deque[str] = deque(maxlen=max_lines)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        with self._lock:
            self._lines.append(msg)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._lines)


class ManagerDashboard:
    def __init__(self, *, refresh_s: float = 0.5, log_lines: int = 500) -> None:
        if not RICH_AVAILABLE:  # pragma: no cover - depends on optional dependency
            raise RuntimeError("rich is required for dashboard support")

        self.refresh_s = max(0.1, float(refresh_s))
        self.log_lines = max(50, int(log_lines))
        self.console = Console()
        self._log_handler = _RingLogHandler(max_lines=self.log_lines)
        self._log_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        )
        self._started_ts: Optional[float] = None
        self._live = Live(self._render({}, {}, {}, False, 0), console=self.console, screen=True, auto_refresh=False)
        self._started = False
        self._orig_handlers: list[logging.Handler] = []
        self._root_logger: Optional[logging.Logger] = None

    def start(self) -> None:
        if self._started:
            return
        root = logging.getLogger()
        self._root_logger = root
        self._orig_handlers = list(root.handlers)
        for handler in list(root.handlers):
            root.removeHandler(handler)
        root.addHandler(self._log_handler)

        try:
            self._started_ts = time.time()
            self._live.start()
        except Exception:
            root.removeHandler(self._log_handler)
            for handler in self._orig_handlers:
                root.addHandler(handler)
            self._root_logger = None
            self._orig_handlers = []
            raise
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return

        self._live.stop()
        if self._root_logger is not None:
            self._root_logger.removeHandler(self._log_handler)
            for handler in self._orig_handlers:
                self._root_logger.addHandler(handler)
        self._started = False

    def update(
        self,
        *,
        task_counts: dict[str, int],
        worker_counts: dict[str, int],
        launch_counts: dict[str, int],
        shutdown_started: bool,
        pending_shutdown_acks: int,
    ) -> None:
        if not self._started:
            return
        renderable = self._render(
            task_counts,
            worker_counts,
            launch_counts,
            shutdown_started,
            pending_shutdown_acks,
        )
        self._live.update(renderable, refresh=True)

    def _render(
        self,
        task_counts: dict[str, int],
        worker_counts: dict[str, int],
        launch_counts: dict[str, int],
        shutdown_started: bool,
        pending_shutdown_acks: int,
    ):
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
        now_ts = time.time()
        elapsed_s = max(0.0, now_ts - (self._started_ts or now_ts))
        eta_s: Optional[float] = None
        if total_tasks > 0 and done > 0 and elapsed_s > 0:
            rate = done / elapsed_s
            if rate > 0:
                eta_s = max(0.0, (total_tasks - done) / rate)

        elapsed_txt = _format_duration(elapsed_s)
        eta_txt = _format_duration(eta_s) if eta_s is not None else "?"
        progress_lines = Group(
            bar,
            Text(
                f"done={done}/{total_tasks} ({pct:.1f}%, {elapsed_txt}<{eta_txt}) "
                f"succeeded={succeeded} failed={failed} in_progress={in_progress} other={other}",
                style="bold",
            ),
        )

        worker_total = sum(worker_counts.values())
        worker_alive = worker_total - worker_counts.get("OFFLINE", 0)

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
                f"Shutdown: in progress",
                f"Pending ShutdownAck: {pending_shutdown_acks}",
                "",
            )

        log_snapshot = self._log_handler.snapshot()
        max_log_lines = max(6, self.console.size.height - 16)
        log_lines = log_snapshot[-max_log_lines:]
        log_text = Text("\n".join(log_lines)) if log_lines else Text("(no logs yet)", style="dim")

        layout = Layout()
        layout.split_column(
            Layout(Panel(progress_lines, title="Task Progress", border_style="cyan"), size=4),
            Layout(Panel(stats, title="Manager Stats", border_style="cyan"), size=7),
            Layout(Rule(style="grey50"), size=1),
            Layout(Panel(log_text, title="Manager Log", border_style="cyan"), ratio=1),
        )
        return layout


def _stacked_bar(*, width: int, values: list[tuple[str, int]], total: int) -> Text:
    width = max(1, int(width))
    total = max(1, int(total))

    raw = []
    for style, value in values:
        frac = max(0.0, float(value) / float(total))
        chars = frac * width
        raw.append((style, value, int(math.floor(chars)), chars))

    used = sum(item[2] for item in raw)
    remaining = width - used
    if remaining > 0:
        raw_sorted_idx = sorted(
            range(len(raw)),
            key=lambda i: (raw[i][3] - raw[i][2]),
            reverse=True,
        )
        for idx in raw_sorted_idx[:remaining]:
            style, value, base, chars = raw[idx]
            raw[idx] = (style, value, base + 1, chars)

    text = Text()
    for style, _value, chars, _ in raw:
        if chars > 0:
            text.append("█" * chars, style=style)
    if len(text.plain) < width:
        text.append("█" * (width - len(text.plain)), style="grey35")
    return text


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "?"
    whole = max(0, int(seconds))
    hours, rem = divmod(whole, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
