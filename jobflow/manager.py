from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .launcher.base import Launcher
from .launcher.lsf import LsfLauncher
from .launcher.multiprocess import MultiprocessLauncher
from .launcher.slurm import SlurmLauncher
from .messages import make_envelope, validate_type_for_direction
from .models import LaunchState, TaskState, WorkerState
from .program import load_program
from .scheduler import FifoScheduler
from .store import Store
from .transport.base import Transport
from .transport.fs_transport import FsTransport
from .transport.zmq_transport import make_manager_transport

logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.store = Store(Path(args.db_path))
        self.scheduler = FifoScheduler()
        self.transport = self._make_transport(args)
        self.launcher = self._make_launcher(args.enable_launcher)
        self.running = True

        self.lease_duration_s = int(args.lease_duration)
        self.heartbeat_interval_s = int(args.heartbeat_interval)
        self.worker_timeout_s = int(args.worker_timeout)
        self.max_inflight_per_worker = 1

        self._last_tick = time.time()
        self._last_launch_poll = 0.0

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info("Received signal %s, stopping manager", signum)
        self.running = False

    def _make_transport(self, args: argparse.Namespace) -> Transport:
        if args.mode == "zmq":
            return make_manager_transport(bind_host=args.bind_host, port=args.port)
        if args.mode == "fs":
            if not args.shared_dir or not args.session_id:
                raise ValueError("--shared-dir and --session-id are required in fs mode")
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

    def bootstrap_tasks(self) -> None:
        program_args = _parse_json_dict(self.args.program_args)
        program = load_program(self.args.program, program_args)

        inserted = 0
        existing = 0
        for task_def in program.generate_tasks():
            status = self.store.upsert_task(task_def.task_id, task_def.spec, task_def.max_retries)
            if status == "inserted":
                inserted += 1
            elif status == "spec_mismatch":
                logger.warning("Task %s exists with different spec_json; keeping stored version", task_def.task_id)
            elif status == "terminal_exists":
                logger.info("Task %s already terminal in DB; skipping", task_def.task_id)
            else:
                existing += 1

        counts = self.store.task_counts()
        logger.info("Loaded tasks: inserted=%s existing=%s state_counts=%s", inserted, existing, counts)

    def maybe_submit_workers_on_start(self) -> None:
        if not self.launcher:
            return
        if self.args.worker_count_on_start <= 0:
            return

        worker_command = self._build_worker_command()
        env = {}
        requested = {"worker_command": worker_command, "mode": self.args.mode}
        records = self.launcher.submit_workers(self.args.worker_count_on_start, worker_command, env, requested)

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
        ]

        if self.args.mode == "zmq":
            manager_host = self.args.manager_host or self.args.bind_host
            cmd.extend(["--manager-host", manager_host])
        else:
            cmd.extend(["--shared-dir", self.args.shared_dir, "--session-id", self.args.session_id])

        return cmd

    def run(self) -> None:
        self.bootstrap_tasks()
        self.maybe_submit_workers_on_start()

        while self.running:
            for msg in self.transport.poll(0.5):
                self._handle_incoming(msg)

            now = time.time()
            if now - self._last_tick >= 1.0:
                self._timer_tick(now)
                self._last_tick = now

        self.transport.close()
        self.store.close()

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

        self._send_to_worker(
            worker_id,
            "Welcome",
            {
                "heartbeat_interval_s": self.heartbeat_interval_s,
                "lease_duration_s": self.lease_duration_s,
                "max_inflight_per_worker": self.max_inflight_per_worker,
            },
        )
        logger.info("Worker registered: %s launch_id=%s batch_job_id=%s", worker_id, linked_launch_id, batch_job_id)

    def _on_heartbeat(self, worker_id: str, payload: dict) -> None:
        lease_id = payload.get("current_lease_id") if isinstance(payload.get("current_lease_id"), str) else None
        self.store.update_worker_heartbeat(worker_id, time.time(), lease_id)

    def _on_request_work(self, worker_id: str) -> None:
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
    parser.add_argument("--mode", "-m", choices=["zmq", "fs"], required=True)
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--port", "-p", type=int, default=5555)
    parser.add_argument("--manager-host", default=None)
    parser.add_argument("--shared-dir", "-s", default=None)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--lease-duration", "-l", type=int, default=120)
    parser.add_argument("--heartbeat-interval", "-i", type=int, default=10)
    parser.add_argument("--worker-timeout", "-t", type=int, default=30)
    parser.add_argument("--db-path", "-d", required=True)
    parser.add_argument("--program", "-P", required=True)
    parser.add_argument("--program-args", "-A", default="{}")
    parser.add_argument("--enable-launcher", choices=["none", "multiprocess", "lsf", "slurm"], default="none")
    parser.add_argument("--launch-stale-timeout", type=int, default=21600)
    parser.add_argument("--launch-poll-interval", type=int, default=60)
    parser.add_argument("--worker-count-on-start", type=int, default=0)
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
