from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .dashboard import ManagerDashboard, RICH_AVAILABLE
from .launcher.base import Launcher
from .launcher.lsf import LsfLauncher
from .launcher.multiprocess import MultiprocessLauncher
from .launcher.slurm import SlurmLauncher
from .messages import make_envelope, validate_type_for_direction
from .models import LaunchState, TaskState, WorkerState
from .program import load_program
from .scheduler import FifoScheduler
from .store import Store
from .telemetry import FileTelemetryPublisher, NoopTelemetryPublisher, TelemetryPublisher, TelemetrySnapshot
from .transport.base import Transport
from .transport.fs_transport import FsTransport
from .transport.zmq_transport import make_manager_transport

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, args: argparse.Namespace, telemetry_publisher: Optional[TelemetryPublisher] = None) -> None:
        self._normalize_args(args)
        self._apply_log_level(args.log_level)
        self.args = args
        self.store = Store(Path(args.db_path))
        self.scheduler = FifoScheduler()
        self.transport = self._make_transport(args)
        self.launcher = self._make_launcher(args.launcher)
        self.running = True

        self.lease_duration_s = int(args.lease_duration)
        self.heartbeat_interval_s = int(args.heartbeat_interval)
        self.worker_timeout_s = int(args.worker_timeout)
        self.shutdown_grace_period_s = int(args.shutdown_grace_period)
        self.max_inflight_per_worker = 1

        self._last_tick = time.time()
        self._last_launch_poll = 0.0
        self._shutdown_started = False
        self._shutdown_deadline_ts: Optional[float] = None
        self._pending_shutdown_acks: set[str] = set()
        self._shutdown_launches_canceled = False
        self._last_shutdown_broadcast_ts = 0.0
        self._last_dashboard_refresh_ts = 0.0
        self.dashboard: Optional[ManagerDashboard] = self._make_dashboard(args)
        self.dashboard_refresh_s = float(args.dashboard_refresh)
        self.telemetry_publisher: TelemetryPublisher = telemetry_publisher or self._make_telemetry_publisher(args)
        self._telemetry_publisher_failed = False
        self._manual_shutdown_requested = False

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _apply_log_level(self, raw_level: str) -> None:
        level = getattr(logging, str(raw_level).upper(), logging.INFO)
        logging.getLogger().setLevel(level)

    def _normalize_args(self, args: argparse.Namespace) -> None:
        if not hasattr(args, "dashboard"):
            args.dashboard = "auto"
        if not hasattr(args, "dashboard_refresh"):
            args.dashboard_refresh = 0.5
        if not hasattr(args, "dashboard_log_lines"):
            args.dashboard_log_lines = 500
        if not hasattr(args, "telemetry_mode"):
            args.telemetry_mode = "off"
        if not hasattr(args, "telemetry_file"):
            args.telemetry_file = None
        if not hasattr(args, "telemetry_queue_size"):
            args.telemetry_queue_size = 2
        if args.mode == "fs":
            if not args.shared_dir:
                args.shared_dir = str(Path.cwd())
            if not args.session_id:
                args.session_id = "jobflow-session"
        if args.telemetry_mode == "file" and not args.telemetry_file:
            if args.mode == "fs":
                telemetry_path = Path(args.shared_dir) / args.session_id / "dashboard_snapshot.json"
            else:
                telemetry_path = Path(args.db_path).with_suffix(".dashboard.json")
            args.telemetry_file = str(telemetry_path)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        if not self._manual_shutdown_requested:
            logger.info("Received signal %s, initiating graceful shutdown", signum)
            self._manual_shutdown_requested = True
            return
        logger.warning("Received second signal %s, forcing immediate manager stop", signum)
        self.running = False

    def _make_transport(self, args: argparse.Namespace) -> Transport:
        if args.mode == "zmq":
            return make_manager_transport(bind_host=args.bind_host, port=args.port)
        if args.mode == "fs":
            return FsTransport(base_dir=Path(args.shared_dir), session_id=args.session_id, endpoint_id="manager")
        raise ValueError(f"Unsupported mode: {args.mode}")

    def _make_launcher(self, mode: str) -> Optional[Launcher]:
        if mode == "none":
            return None
        if mode == "multiprocess":
            return MultiprocessLauncher()
        if mode == "lsf":
            return LsfLauncher()
        if mode == "slurm":
            return SlurmLauncher()
        raise ValueError(f"Unsupported launcher: {mode}")

    def _make_dashboard(self, args: argparse.Namespace) -> Optional[ManagerDashboard]:
        mode = args.dashboard
        if mode not in {"auto", "on", "off"}:
            raise ValueError("--dashboard must be one of: auto, on, off")
        if mode == "off":
            return None

        tty_ok = bool(sys.stdout.isatty() and sys.stderr.isatty())
        if mode == "auto" and not tty_ok:
            return None
        if not RICH_AVAILABLE:
            if mode == "on":
                raise RuntimeError("Dashboard requested but 'rich' is not installed.")
            return None
        if mode == "on" and not tty_ok:
            raise RuntimeError("Dashboard requires an interactive TTY.")

        return ManagerDashboard(
            refresh_s=float(args.dashboard_refresh),
            log_lines=int(args.dashboard_log_lines),
        )

    def _make_telemetry_publisher(self, args: argparse.Namespace) -> TelemetryPublisher:
        if args.telemetry_mode == "off":
            return NoopTelemetryPublisher()
        if args.telemetry_mode == "file":
            return FileTelemetryPublisher(
                Path(args.telemetry_file),
                max_queue=int(args.telemetry_queue_size),
            )
        raise ValueError(f"Unsupported telemetry mode: {args.telemetry_mode}")

    def bootstrap_tasks(self) -> None:
        program_args = _parse_json_dict(self.args.program_args)
        program = load_program(self.args.program, program_args)
        logger.info("Starting task generation with program=%s", self.args.program)
        self._refresh_dashboard(force=True)

        raw_tasks = program.generate_tasks()
        if isinstance(raw_tasks, list):
            task_defs = raw_tasks
        else:
            # Backward compatibility for older programs that still yield.
            task_defs = list(raw_tasks)
            logger.warning(
                "Program %s returned non-list tasks iterable; converting to list for bulk task API compatibility.",
                self.args.program,
            )

        inserted = 0
        existing = 0
        spec_mismatch = 0
        terminal_exists = 0
        last_ui_refresh_ts = time.time()
        chunk_size = 2000
        for start in range(0, len(task_defs), chunk_size):
            chunk = task_defs[start : start + chunk_size]
            status = self.store.bulk_upsert_tasks(chunk)
            inserted += status["inserted"]
            existing += status["exists"]
            spec_mismatch += status["spec_mismatch"]
            terminal_exists += status["terminal_exists"]
            now = time.time()
            if now - last_ui_refresh_ts >= self.dashboard_refresh_s:
                self._refresh_dashboard(now=now, force=True)
                last_ui_refresh_ts = now

        counts = self.store.task_counts()
        logger.info(
            "Task generation completed: inserted=%s existing=%s spec_mismatch=%s terminal_exists=%s state_counts=%s",
            inserted,
            existing,
            spec_mismatch,
            terminal_exists,
            counts,
        )
        self._refresh_dashboard(force=True)

    def _submit_workers(self, count: int) -> None:
        if not self.launcher or count <= 0:
            return

        worker_command = self._build_worker_command()
        env = {}
        requested = {"worker_command": worker_command, "mode": self.args.mode}
        if self.args.launcher == "lsf":
            requested.update(
                {
                    "lsf_queue": self.args.lsf_queue,
                    "lsf_nproc": self.args.lsf_nproc,
                    "lsf_mem": self.args.lsf_mem,
                    "lsf_env_script": self.args.lsf_env_script,
                }
            )
        batch_size = 10
        remaining = int(count)
        while remaining > 0:
            this_batch = min(batch_size, remaining)
            records = self.launcher.submit_workers(this_batch, worker_command, env, requested)
            for rec in records:
                self.store.upsert_launch(
                    rec.launch_id,
                    system=rec.system,
                    state=rec.state,
                    requested=requested,
                    batch_job_id=rec.batch_job_id,
                    worker_id_expected=rec.worker_id_expected,
                )
                logger.info(
                    "Launch submitted launch_id=%s batch_job_id=%s state=%s",
                    rec.launch_id,
                    rec.batch_job_id,
                    rec.state.value,
                )
            remaining -= this_batch
            self._refresh_dashboard(force=True)

    def _has_remaining_tasks(self) -> bool:
        counts = self.store.task_counts()
        total = sum(counts.values())
        terminal = (
            counts.get(TaskState.SUCCEEDED.value, 0)
            + counts.get(TaskState.FAILED.value, 0)
            + counts.get(TaskState.CANCELED.value, 0)
        )
        return total > 0 and terminal < total

    def _reconcile_worker_pool(self) -> None:
        if self.launcher is None or self._shutdown_started:
            return
        target = max(0, int(self.args.workers))
        if target <= 0:
            return
        if not self._has_remaining_tasks():
            return

        online_workers = self.store.count_non_offline_workers()
        pending_launches = self.store.count_pending_launches()
        in_pool = online_workers + pending_launches
        missing = target - in_pool
        if missing <= 0:
            return

        logger.info(
            "Worker pool below target: target=%s online=%s pending_launches=%s; submitting %s workers",
            target,
            online_workers,
            pending_launches,
            missing,
        )
        self._submit_workers(missing)

    def _build_worker_command(self) -> list[str]:
        cmd = [
            sys.executable,
            "-m",
            "jobflow.worker",
            "--mode",
            self.args.mode,
            "--program",
            self.args.program,
            "--program-args",
            self.args.program_args,
            "--log-level",
            self.args.log_level,
            "--port",
            str(self.args.port),
            "--manager-timeout-minutes",
            str(self.args.worker_manager_timeout_minutes),
        ]

        if self.args.mode == "zmq":
            manager_host = self.args.manager_host or self.args.bind_host
            cmd.extend(["--manager-host", manager_host])
        else:
            cmd.extend(["--shared-dir", self.args.shared_dir, "--session-id", self.args.session_id])

        return cmd

    def run(self) -> dict[str, int]:
        try:
            if self.dashboard:
                self.dashboard.start()
            self._refresh_dashboard(force=True)
            self.bootstrap_tasks()
            self._reconcile_worker_pool()
            self._refresh_dashboard(force=True)

            while self.running:
                for msg in self.transport.poll(0.5):
                    self._handle_incoming(msg)

                now = time.time()
                if now - self._last_tick >= 1.0:
                    self._timer_tick(now)
                    self._last_tick = now
                self._refresh_dashboard(now=now)

            summary = self.store.task_counts()
            self._refresh_dashboard(force=True)
            logger.info("Manager exiting with task summary: %s", summary)
            return summary
        finally:
            if self.dashboard:
                self.dashboard.stop()
            try:
                self.telemetry_publisher.close()
            except Exception:
                logger.exception("Telemetry publisher close failed.")
            self.transport.close()
            self.store.close()

    def _refresh_dashboard(self, *, now: Optional[float] = None, force: bool = False) -> None:
        if self.dashboard is None and isinstance(self.telemetry_publisher, NoopTelemetryPublisher):
            return
        ts = now if now is not None else time.time()
        if not force and ts - self._last_dashboard_refresh_ts < self.dashboard_refresh_s:
            return
        self._last_dashboard_refresh_ts = ts
        task_counts = self.store.task_counts()
        worker_counts = self.store.worker_counts()
        launch_counts = self.store.launch_counts()

        if self.dashboard is not None:
            self.dashboard.update(
                task_counts=task_counts,
                worker_counts=worker_counts,
                launch_counts=launch_counts,
                shutdown_started=self._shutdown_started,
                pending_shutdown_acks=len(self._pending_shutdown_acks),
            )

        snapshot = TelemetrySnapshot(
            ts=ts,
            manager_pid=os.getpid(),
            task_counts=task_counts,
            worker_counts=worker_counts,
            launch_counts=launch_counts,
            shutdown_started=self._shutdown_started,
            pending_shutdown_acks=len(self._pending_shutdown_acks),
        )
        self._publish_snapshot(snapshot)

    def _publish_snapshot(self, snapshot: TelemetrySnapshot) -> None:
        if self._telemetry_publisher_failed:
            return
        try:
            self.telemetry_publisher.publish(snapshot)
        except Exception:
            self._telemetry_publisher_failed = True
            logger.exception("Telemetry publisher failed; disabling telemetry publishing for this manager run.")

    def _timer_tick(self, now: float) -> None:
        cutoff = now - self.worker_timeout_s
        stale_workers = self.store.workers_last_seen_before(cutoff)
        for row in stale_workers:
            worker_id = row["worker_id"]
            self.store.mark_worker_offline(worker_id)
            logger.info("Marked worker offline: %s", worker_id)

        expired = self.store.expire_leases(now)
        for task_id, lease_id, worker_id, attempt in expired:
            task = self.store.get_task(task_id)
            if task is None:
                continue
            if task["lease_id"] != lease_id:
                continue
            if task["state"] not in {TaskState.ASSIGNED.value, TaskState.RUNNING.value}:
                continue

            if attempt < task["max_retries"]:
                self.store.set_task_state(
                    task_id,
                    TaskState.QUEUED,
                    attempt=attempt + 1,
                    assigned_worker_id=None,
                    lease_id=None,
                    lease_expires_at_ts=None,
                    error={"reason": "lease_expired", "worker_id": worker_id, "lease_id": lease_id},
                )
                logger.warning("Lease expired, re-queued task=%s attempt=%s", task_id, attempt + 1)
            else:
                self.store.set_task_state(
                    task_id,
                    TaskState.FAILED,
                    attempt=attempt,
                    assigned_worker_id=None,
                    lease_id=None,
                    lease_expires_at_ts=None,
                    error={"reason": "lease_expired", "worker_id": worker_id, "lease_id": lease_id},
                    finished_ts=now,
                )
                logger.error("Lease expired with retries exhausted task=%s", task_id)

            worker = self.store.get_worker(worker_id)
            if worker is not None and worker["current_lease_id"] == lease_id:
                if worker["state"] == WorkerState.OFFLINE.value:
                    self.store.set_worker_state(worker_id, WorkerState.OFFLINE, current_lease_id=None)
                else:
                    self.store.set_worker_state(worker_id, WorkerState.IDLE, current_lease_id=None)

        self._tick_launchers(now)
        self._reconcile_worker_pool()
        self._maybe_start_shutdown(now)
        self._check_shutdown_progress(now)

    def _maybe_start_shutdown(self, now: float) -> None:
        if self._shutdown_started:
            return
        if self._manual_shutdown_requested:
            self._start_shutdown(now, reason="manual request")
            return
        if not self._all_tasks_terminal():
            return
        self._start_shutdown(now, reason="all tasks terminal")

    def _start_shutdown(self, now: float, *, reason: str) -> None:
        self._shutdown_started = True
        self._shutdown_deadline_ts = now + self.shutdown_grace_period_s

        workers = self.store.list_non_offline_workers()
        self._pending_shutdown_acks = {row["worker_id"] for row in workers}
        logger.info(
            "Initiating shutdown handshake (%s) with %s workers",
            reason,
            len(self._pending_shutdown_acks),
        )

        for worker_id in self._pending_shutdown_acks:
            self.store.set_worker_state(worker_id, WorkerState.DRAINING)
        self._broadcast_shutdown_to_pending(now)

    def _all_tasks_terminal(self) -> bool:
        counts = self.store.task_counts()
        total = sum(counts.values())
        terminal = (
            counts.get(TaskState.SUCCEEDED.value, 0)
            + counts.get(TaskState.FAILED.value, 0)
            + counts.get(TaskState.CANCELED.value, 0)
        )
        return total > 0 and terminal == total

    def _broadcast_shutdown_to_pending(self, now: float) -> None:
        if not self._pending_shutdown_acks:
            return
        for worker_id in self._pending_shutdown_acks:
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})
        self._last_shutdown_broadcast_ts = now

    def _check_shutdown_progress(self, now: float) -> None:
        if not self._shutdown_started:
            return

        for worker_id in list(self._pending_shutdown_acks):
            worker = self.store.get_worker(worker_id)
            if worker is None or worker["state"] == WorkerState.OFFLINE.value:
                self._pending_shutdown_acks.discard(worker_id)

        if not self._pending_shutdown_acks:
            logger.info("All workers shutdown acknowledged or offline.")
            self._force_cancel_remaining_launches()
            self.running = False
            return

        if now - self._last_shutdown_broadcast_ts >= 2.0:
            self._broadcast_shutdown_to_pending(now)

        if self._shutdown_deadline_ts is not None and now >= self._shutdown_deadline_ts:
            logger.warning(
                "Shutdown grace period expired with %s pending worker acks; forcing launch cancel",
                len(self._pending_shutdown_acks),
            )
            self._force_cancel_remaining_launches()
            self.running = False

    def _force_cancel_remaining_launches(self) -> None:
        if self._shutdown_launches_canceled:
            return
        self._shutdown_launches_canceled = True
        if self.launcher is None:
            return

        for row in self.store.list_launches_for_cancel():
            batch_job_id = row["batch_job_id"]
            if not batch_job_id:
                continue
            launch_id = row["launch_id"]
            try:
                self.launcher.cancel(str(batch_job_id))
                self.store.mark_launch_state(launch_id, LaunchState.CANCELED)
            except Exception:
                logger.exception("Failed canceling launched worker batch_job_id=%s launch_id=%s", batch_job_id, launch_id)

    def _tick_launchers(self, now: float) -> None:
        if self.launcher and now - self._last_launch_poll >= self.args.launch_poll_interval:
            rows = self.store.list_launches_for_poll()
            ids = [r["batch_job_id"] for r in rows if r["batch_job_id"]]
            status_by_job = self.launcher.poll(ids)
            for row in rows:
                batch_job_id = row["batch_job_id"]
                if batch_job_id not in status_by_job:
                    continue
                status = status_by_job[batch_job_id]["state"]
                if status == "PENDING":
                    self.store.mark_launch_state(row["launch_id"], LaunchState.PENDING)
                elif status == "RUNNING":
                    self.store.mark_launch_state(row["launch_id"], LaunchState.RUNNING)
                elif status == "FAILED":
                    self.store.mark_launch_state(row["launch_id"], LaunchState.FAILED)
            self._last_launch_poll = now

        stale_after = float(self.args.launch_stale_timeout)
        for row in self.store.list_unresolved_launches():
            if now - row["submitted_ts"] > stale_after:
                self.store.mark_launch_state(row["launch_id"], LaunchState.STALE)
                logger.warning("Marked launch stale launch_id=%s", row["launch_id"])

    def _handle_incoming(self, msg: dict) -> None:
        if msg.get("dst", {}).get("id") != "manager":
            logger.debug("Ignoring message for different destination: %s", msg.get("dst"))
            return
        if not validate_type_for_direction(msg):
            logger.warning("Ignoring invalid message direction/type msg_id=%s type=%s", msg.get("msg_id"), msg.get("type"))
            return

        msg_id = msg["msg_id"]
        if not self.store.dedup_check_and_record(msg_id):
            logger.debug("Duplicate message ignored msg_id=%s", msg_id)
            return

        msg_type = msg["type"]
        src_id = msg["src"]["id"]
        payload = msg["payload"]

        if msg_type == "Register":
            self._on_register(src_id, payload)
        elif msg_type == "Heartbeat":
            self._on_heartbeat(src_id, payload)
        elif msg_type == "RequestWork":
            self._on_request_work(src_id)
        elif msg_type == "TaskStarted":
            self._on_task_started(src_id, payload)
        elif msg_type == "TaskProgress":
            self._on_task_progress(src_id, payload)
        elif msg_type == "TaskFinished":
            self._on_task_finished(src_id, payload)
        elif msg_type == "TaskFailed":
            self._on_task_failed(src_id, payload)
        elif msg_type == "ShutdownAck":
            self._on_shutdown_ack(src_id, payload)
        else:
            logger.warning("Unknown worker message type: %s", msg_type)

    def _send_to_worker(self, worker_id: str, msg_type: str, payload: dict) -> None:
        msg = make_envelope(
            src_role="manager",
            src_id="manager",
            dst_role="worker",
            dst_id=worker_id,
            msg_type=msg_type,
            payload=payload,
        )
        self.transport.send(worker_id, msg)

    def _send_welcome(self, worker_id: str) -> None:
        self._send_to_worker(
            worker_id,
            "Welcome",
            {
                "heartbeat_interval_s": self.heartbeat_interval_s,
                "lease_duration_s": self.lease_duration_s,
                "max_inflight_per_worker": self.max_inflight_per_worker,
            },
        )

    def _on_register(self, worker_id: str, payload: dict) -> None:
        now = time.time()
        capabilities = payload.get("capabilities") if isinstance(payload.get("capabilities"), dict) else {}
        launch_id = payload.get("launch_id")
        batch_job_id = payload.get("batch_job_id")

        linked_launch_id: Optional[str] = None
        if isinstance(launch_id, str):
            launch_row = self.store.find_launch(launch_id)
            if launch_row:
                linked_launch_id = launch_id
                self.store.mark_launch_state(launch_id, LaunchState.ONLINE)
        elif isinstance(batch_job_id, str):
            launch_row = self.store.find_launch_by_batch_job_id(batch_job_id)
            if launch_row:
                linked_launch_id = launch_row["launch_id"]
                self.store.mark_launch_state(linked_launch_id, LaunchState.ONLINE)

        self.store.upsert_worker(
            worker_id,
            state=WorkerState.IDLE,
            capabilities=capabilities,
            last_heartbeat_ts=now,
            current_lease_id=None,
            batch_job_id=batch_job_id if isinstance(batch_job_id, str) else None,
            launch_id=linked_launch_id,
        )
        if self._shutdown_started:
            self.store.set_worker_state(worker_id, WorkerState.DRAINING)
            self._pending_shutdown_acks.add(worker_id)
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})
        else:
            self._send_welcome(worker_id)
        logger.info("Worker registered: %s launch_id=%s batch_job_id=%s", worker_id, linked_launch_id, batch_job_id)

    def _on_heartbeat(self, worker_id: str, payload: dict) -> None:
        lease_id = payload.get("current_lease_id") if isinstance(payload.get("current_lease_id"), str) else None
        self.store.update_worker_heartbeat(worker_id, time.time(), lease_id)
        if self._shutdown_started:
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})
        else:
            # Heartbeat reply lets workers detect manager liveness.
            self._send_welcome(worker_id)

    def _on_request_work(self, worker_id: str) -> None:
        if self._shutdown_started:
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})
            return

        worker = self.store.get_worker(worker_id)
        if worker is None:
            logger.warning("RequestWork from unknown worker: %s", worker_id)
            return
        if worker["state"] in {WorkerState.BUSY.value, WorkerState.OFFLINE.value, WorkerState.DRAINING.value}:
            return

        task = self.scheduler.choose_next_task(self.store)
        if task is None:
            return

        now = time.time()
        attempt = int(task["attempt"])
        lease_expires = now + self.lease_duration_s
        lease_id = self.store.create_lease(task["task_id"], worker_id, attempt, lease_expires)

        self.store.set_task_state(
            task["task_id"],
            TaskState.ASSIGNED,
            attempt=attempt,
            assigned_worker_id=worker_id,
            lease_id=lease_id,
            lease_expires_at_ts=lease_expires,
        )
        self.store.set_worker_state(worker_id, WorkerState.BUSY, current_lease_id=lease_id)

        spec = json.loads(task["spec_json"])
        self._send_to_worker(
            worker_id,
            "Assign",
            {
                "lease_id": lease_id,
                "task_id": task["task_id"],
                "attempt": attempt,
                "spec": spec,
                "lease_expires_at_ts": lease_expires,
            },
        )
        logger.info("Assigned task=%s to worker=%s lease=%s attempt=%s", task["task_id"], worker_id, lease_id, attempt)

    def _on_task_started(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        lease_id = payload.get("lease_id")
        if not isinstance(task_id, str) or not isinstance(lease_id, str):
            return

        task = self.store.get_task(task_id)
        if task is None or task["lease_id"] != lease_id or task["assigned_worker_id"] != worker_id:
            logger.debug("Ignoring TaskStarted with stale lease task=%s lease=%s", task_id, lease_id)
            return

        self.store.set_task_state(task_id, TaskState.RUNNING, started_ts=time.time(), attempt=int(task["attempt"]), assigned_worker_id=worker_id, lease_id=lease_id, lease_expires_at_ts=task["lease_expires_at_ts"])

    def _on_task_progress(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        lease_id = payload.get("lease_id")
        progress = payload.get("progress")
        logger.debug("Progress worker=%s task=%s lease=%s progress=%s", worker_id, task_id, lease_id, progress)

    def _on_task_finished(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        lease_id = payload.get("lease_id")
        result_ref = payload.get("result_ref")
        if not isinstance(task_id, str) or not isinstance(lease_id, str) or not isinstance(result_ref, dict):
            return

        task = self.store.get_task(task_id)
        if task is None or task["lease_id"] != lease_id or task["assigned_worker_id"] != worker_id:
            logger.info("Ignoring TaskFinished with stale lease task=%s lease=%s", task_id, lease_id)
            return

        self.store.remove_lease(lease_id)
        self.store.set_task_state(
            task_id,
            TaskState.SUCCEEDED,
            attempt=int(task["attempt"]),
            assigned_worker_id=None,
            lease_id=None,
            lease_expires_at_ts=None,
            result_ref=result_ref,
            finished_ts=time.time(),
        )
        self.store.set_worker_state(worker_id, WorkerState.IDLE, current_lease_id=None)
        if self._shutdown_started:
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})
        logger.info("Task succeeded task=%s worker=%s", task_id, worker_id)

    def _on_task_failed(self, worker_id: str, payload: dict) -> None:
        task_id = payload.get("task_id")
        lease_id = payload.get("lease_id")
        error = payload.get("error")
        if not isinstance(task_id, str) or not isinstance(lease_id, str) or not isinstance(error, dict):
            return

        task = self.store.get_task(task_id)
        if task is None or task["lease_id"] != lease_id or task["assigned_worker_id"] != worker_id:
            logger.info("Ignoring TaskFailed with stale lease task=%s lease=%s", task_id, lease_id)
            return

        self.store.remove_lease(lease_id)
        attempt = int(task["attempt"])
        max_retries = int(task["max_retries"])

        if attempt < max_retries:
            self.store.set_task_state(
                task_id,
                TaskState.QUEUED,
                attempt=attempt + 1,
                assigned_worker_id=None,
                lease_id=None,
                lease_expires_at_ts=None,
                error=error,
            )
            logger.warning("Task failed and re-queued task=%s attempt=%s", task_id, attempt + 1)
        else:
            self.store.set_task_state(
                task_id,
                TaskState.FAILED,
                attempt=attempt,
                assigned_worker_id=None,
                lease_id=None,
                lease_expires_at_ts=None,
                error=error,
                finished_ts=time.time(),
            )
            logger.error("Task failed permanently task=%s", task_id)

        self.store.set_worker_state(worker_id, WorkerState.IDLE, current_lease_id=None)
        if self._shutdown_started:
            self._send_to_worker(worker_id, "Shutdown", {"graceful": True})

    def _on_shutdown_ack(self, worker_id: str, payload: dict) -> None:
        reason = payload.get("reason")
        self._pending_shutdown_acks.discard(worker_id)
        self.store.mark_worker_offline(worker_id)
        logger.info("Received ShutdownAck from worker=%s reason=%s", worker_id, reason)
        self._check_shutdown_progress(time.time())


def _parse_json_dict(raw: str) -> dict:
    try:
        out = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    if not isinstance(out, dict):
        raise ValueError("JSON value must be an object")
    return out


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JobFlow manager process")
    parser.add_argument("--mode", "-m", choices=["zmq", "fs"], default="fs")
    parser.add_argument("--dashboard", choices=["auto", "on", "off"], default="auto")
    parser.add_argument("--dashboard-refresh", type=float, default=0.5)
    parser.add_argument("--dashboard-log-lines", type=int, default=500)
    parser.add_argument("--telemetry-mode", choices=["off", "file"], default="off")
    parser.add_argument("--telemetry-file", default=None)
    parser.add_argument("--telemetry-queue-size", type=int, default=2)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--port", "-p", type=int, default=5555)
    parser.add_argument("--manager-host", default=None)
    parser.add_argument("--shared-dir", "-s", default=None)
    parser.add_argument("--session-id", default="jobflow-session")
    parser.add_argument("--lease-duration", "-l", type=int, default=1800)
    parser.add_argument("--heartbeat-interval", "-i", type=int, default=10)
    parser.add_argument("--worker-timeout", "-t", type=int, default=60)
    parser.add_argument("--shutdown-grace-period", type=int, default=60)
    parser.add_argument("--worker-manager-timeout-minutes", type=float, default=3.0)
    parser.add_argument("--db-path", "-d", default="jobflow_manager.db")
    parser.add_argument("--program", "-P", required=True)
    parser.add_argument("--program-args", "-A", default="{}")
    parser.add_argument("--launcher", choices=["none", "multiprocess", "lsf", "slurm"], default="multiprocess")
    parser.add_argument("--launch-stale-timeout", type=int, default=21600)
    parser.add_argument("--launch-poll-interval", type=int, default=30)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--lsf-queue", default="long")
    parser.add_argument("--lsf-nproc", type=int, default=10)
    parser.add_argument("--lsf-mem", default="20GB")
    parser.add_argument("--lsf-env-script", default="~/start_nnunetv2.sh")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    manager = Manager(args)
    manager.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
