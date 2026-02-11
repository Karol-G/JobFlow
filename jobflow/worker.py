from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import queue
import signal
import socket
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from pathlib import Path
from typing import Optional

from .messages import make_envelope, validate_type_for_direction
from .program import load_program
from .transport.base import Transport
from .transport.fs_transport import FsTransport
from .transport.zmq_transport import make_worker_transport

logger = logging.getLogger(__name__)


class DedupCache:
    def __init__(self, max_size: int = 50_000) -> None:
        self.max_size = max_size
        self.seen: set[str] = set()
        self.order: deque[str] = deque()

    def check_and_add(self, msg_id: str) -> bool:
        if msg_id in self.seen:
            return False
        self.seen.add(msg_id)
        self.order.append(msg_id)
        if len(self.order) > self.max_size:
            old = self.order.popleft()
            self.seen.discard(old)
        return True


class Worker:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.worker_id = args.worker_id or _default_worker_id()
        self.transport = self._make_transport(args)
        self.program = load_program(args.program, _parse_json_dict(args.program_args))

        self.running = True
        self.shutdown_requested = False
        self.current_assignment: Optional[dict] = None

        self.heartbeat_interval_s = int(args.heartbeat_interval) if args.heartbeat_interval is not None else 10
        self.lease_duration_s = 120
        self.request_interval_s = 2.0

        self._last_heartbeat = 0.0
        self._last_request = 0.0
        self._dedup = DedupCache()

        self._events: queue.Queue[dict] = queue.Queue()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info("Received signal %s, stopping worker", signum)
        self.running = False

    def _make_transport(self, args: argparse.Namespace) -> Transport:
        if args.mode == "zmq":
            if not args.manager_host:
                raise ValueError("--manager-host is required in zmq mode")
            return make_worker_transport(self.worker_id, manager_host=args.manager_host, port=args.port)
        if args.mode == "fs":
            if not args.shared_dir or not args.session_id:
                raise ValueError("--shared-dir and --session-id are required in fs mode")
            return FsTransport(base_dir=Path(args.shared_dir), session_id=args.session_id, endpoint_id=self.worker_id)
        raise ValueError(f"Unsupported mode: {args.mode}")

    def run(self) -> None:
        self._send_register()
        self._wait_for_welcome(timeout_s=2.0)

        while self.running:
            now = time.time()
            if now - self._last_heartbeat >= self.heartbeat_interval_s:
                self._send_heartbeat()
                self._last_heartbeat = now

            if self.current_assignment is None and not self.shutdown_requested and now - self._last_request >= self.request_interval_s:
                self._send("RequestWork", {})
                self._last_request = now

            self._process_events()
            self._poll_messages()

            if self.shutdown_requested and self.current_assignment is None:
                break

        self.transport.close()

    def _send(self, msg_type: str, payload: dict) -> None:
        msg = make_envelope(
            src_role="worker",
            src_id=self.worker_id,
            dst_role="manager",
            dst_id="manager",
            msg_type=msg_type,
            payload=payload,
        )
        self.transport.send("manager", msg)

    def _send_register(self) -> None:
        launch_id = self.args.launch_id or os.getenv("JOBFLOW_LAUNCH_ID")
        batch_job_id = self.args.batch_job_id or os.getenv("LSB_JOBID") or os.getenv("SLURM_JOB_ID")

        payload = {
            "capabilities": {
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "platform": platform.platform(),
                "python": platform.python_version(),
            }
        }
        if launch_id:
            payload["launch_id"] = launch_id
        if batch_job_id:
            payload["batch_job_id"] = str(batch_job_id)

        self._send("Register", payload)

    def _send_heartbeat(self) -> None:
        payload = {}
        if self.current_assignment is not None:
            payload["current_lease_id"] = self.current_assignment["lease_id"]
        self._send("Heartbeat", payload)

    def _poll_messages(self) -> None:
        for msg in self.transport.poll(0.2):
            self._handle_message(msg)

    def _wait_for_welcome(self, timeout_s: float) -> None:
        deadline = time.time() + timeout_s
        while self.running and time.time() < deadline:
            msgs = self.transport.poll(0.2)
            if not msgs:
                continue
            for msg in msgs:
                self._handle_message(msg)
                if msg.get("type") == "Welcome":
                    return

    def _handle_message(self, msg: dict) -> None:
        dst_id = msg.get("dst", {}).get("id")
        if dst_id not in {self.worker_id, "*"}:
            logger.debug("Ignoring message for different worker dst=%s", dst_id)
            return
        if not validate_type_for_direction(msg):
            logger.warning("Ignoring invalid manager message msg_id=%s type=%s", msg.get("msg_id"), msg.get("type"))
            return

        if not self._dedup.check_and_add(msg["msg_id"]):
            logger.debug("Duplicate manager message ignored msg_id=%s", msg["msg_id"])
            return

        msg_type = msg["type"]
        payload = msg["payload"]

        if msg_type == "Welcome":
            self._on_welcome(payload)
        elif msg_type == "Assign":
            self._on_assign(payload)
        elif msg_type == "Cancel":
            self._on_cancel(payload)
        elif msg_type == "Shutdown":
            self._on_shutdown(payload)
        else:
            logger.warning("Unknown manager message type: %s", msg_type)

    def _on_welcome(self, payload: dict) -> None:
        if self.args.heartbeat_interval is None:
            hb = payload.get("heartbeat_interval_s")
            if isinstance(hb, int) and hb > 0:
                self.heartbeat_interval_s = hb
        lease_duration = payload.get("lease_duration_s")
        if isinstance(lease_duration, int) and lease_duration > 0:
            self.lease_duration_s = lease_duration

    def _on_assign(self, payload: dict) -> None:
        if self.current_assignment is not None:
            logger.info("Ignoring Assign while task is running")
            return

        required = {"lease_id", "task_id", "attempt", "spec"}
        if not required.issubset(payload):
            logger.warning("Assign missing required fields: %s", required - set(payload))
            return
        if not isinstance(payload["spec"], dict):
            logger.warning("Assign.spec must be a dict")
            return

        assignment = {
            "lease_id": payload["lease_id"],
            "task_id": payload["task_id"],
            "attempt": int(payload["attempt"]),
            "spec": payload["spec"],
        }
        self.current_assignment = assignment
        self._send(
            "TaskStarted",
            {
                "lease_id": assignment["lease_id"],
                "task_id": assignment["task_id"],
                "attempt": assignment["attempt"],
            },
        )

        thread = threading.Thread(target=self._run_task, args=(assignment,), daemon=True)
        thread.start()

    def _on_cancel(self, payload: dict) -> None:
        task_id = payload.get("task_id")
        reason = payload.get("reason")
        logger.info("Received cancel task_id=%s reason=%s (cooperative cancel is not implemented in v1)", task_id, reason)

    def _on_shutdown(self, payload: dict) -> None:
        graceful = bool(payload.get("graceful", True))
        if graceful and self.current_assignment is not None:
            self.shutdown_requested = True
            return
        self.running = False

    def _run_task(self, assignment: dict) -> None:
        last_progress_ts = 0.0

        def progress_cb(progress: float, metrics: Optional[dict] = None) -> None:
            nonlocal last_progress_ts
            now = time.time()
            if now - last_progress_ts < 1.0:
                return
            last_progress_ts = now
            self._events.put(
                {
                    "kind": "progress",
                    "lease_id": assignment["lease_id"],
                    "task_id": assignment["task_id"],
                    "attempt": assignment["attempt"],
                    "progress": float(progress),
                    "metrics": metrics if isinstance(metrics, dict) else None,
                }
            )

        try:
            result_ref = self.program.execute_task(assignment["spec"], progress_cb)
            if not isinstance(result_ref, dict):
                result_ref = {"result": result_ref}
            self._events.put(
                {
                    "kind": "finished",
                    "lease_id": assignment["lease_id"],
                    "task_id": assignment["task_id"],
                    "attempt": assignment["attempt"],
                    "result_ref": result_ref,
                }
            )
        except Exception as exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            self._events.put(
                {
                    "kind": "failed",
                    "lease_id": assignment["lease_id"],
                    "task_id": assignment["task_id"],
                    "attempt": assignment["attempt"],
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "traceback_truncated": tb[-4000:],
                    },
                }
            )

    def _process_events(self) -> None:
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break

            if self.current_assignment is None:
                continue

            if event["lease_id"] != self.current_assignment["lease_id"]:
                continue

            if event["kind"] == "progress":
                payload = {
                    "lease_id": event["lease_id"],
                    "task_id": event["task_id"],
                    "attempt": event["attempt"],
                    "progress": event["progress"],
                }
                if event["metrics"] is not None:
                    payload["metrics"] = event["metrics"]
                self._send("TaskProgress", payload)
            elif event["kind"] == "finished":
                self._send(
                    "TaskFinished",
                    {
                        "lease_id": event["lease_id"],
                        "task_id": event["task_id"],
                        "attempt": event["attempt"],
                        "result_ref": event["result_ref"],
                    },
                )
                self.current_assignment = None
            elif event["kind"] == "failed":
                self._send(
                    "TaskFailed",
                    {
                        "lease_id": event["lease_id"],
                        "task_id": event["task_id"],
                        "attempt": event["attempt"],
                        "error": event["error"],
                    },
                )
                self.current_assignment = None


def _default_worker_id() -> str:
    host = socket.gethostname().split(".")[0]
    return f"{host}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def _parse_json_dict(raw: str) -> dict:
    try:
        out = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    if not isinstance(out, dict):
        raise ValueError("JSON value must be an object")
    return out


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JobFlow worker process")
    parser.add_argument("--mode", "-m", choices=["zmq", "fs"], required=True)
    parser.add_argument("--manager-host", default=None)
    parser.add_argument("--port", "-p", type=int, default=5555)
    parser.add_argument("--shared-dir", "-s", default=None)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--worker-id", "-w", default=None)
    parser.add_argument("--launch-id", default=None)
    parser.add_argument("--batch-job-id", default=None)
    parser.add_argument("--program", "-P", required=True)
    parser.add_argument("--program-args", "-A", default="{}")
    parser.add_argument("--heartbeat-interval", "-i", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    worker = Worker(args)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
