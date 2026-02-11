from __future__ import annotations

import logging
import os
import subprocess
import time
import uuid

from .base import Launcher
from ..models import LaunchRecord, LaunchState

logger = logging.getLogger(__name__)


class MultiprocessLauncher(Launcher):
    """
    Local launcher that starts workers as child processes.

    This is useful for local development where LSF/SLURM are unavailable.
    """

    def __init__(self) -> None:
        self._processes: dict[str, subprocess.Popen] = {}

    def submit_workers(
        self,
        count: int,
        worker_command: list[str],
        env: dict[str, str],
        requested: dict,
    ) -> list[LaunchRecord]:
        records: list[LaunchRecord] = []
        now = time.time()
        base_env = os.environ.copy()
        base_env.update(env)

        for _ in range(count):
            launch_id = str(uuid.uuid4())
            cmd = list(worker_command) + ["--launch-id", launch_id]
            try:
                proc = subprocess.Popen(cmd, env=base_env)
                batch_job_id = str(proc.pid)
                self._processes[batch_job_id] = proc
                state = LaunchState.SUBMITTED
            except Exception:
                logger.exception("Failed submitting multiprocess worker")
                batch_job_id = None
                state = LaunchState.FAILED

            records.append(
                LaunchRecord(
                    launch_id=launch_id,
                    system="multiprocess",
                    batch_job_id=batch_job_id,
                    submitted_ts=now,
                    last_update_ts=now,
                    state=state,
                    requested_json="{}",
                    worker_id_expected=None,
                )
            )

        return records

    def cancel(self, batch_job_id: str) -> None:
        proc = self._processes.get(str(batch_job_id))
        if proc is None:
            return
        if proc.poll() is not None:
            return

        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)

    def poll(self, batch_job_ids: list[str]) -> dict[str, dict]:
        if not batch_job_ids:
            return {}

        out: dict[str, dict] = {}
        for job_id in batch_job_ids:
            proc = self._processes.get(str(job_id))
            if proc is not None:
                rc = proc.poll()
                if rc is None:
                    out[job_id] = {"state": "RUNNING", "raw": "alive"}
                else:
                    out[job_id] = {"state": "FAILED", "raw": f"exit_{rc}"}
                continue

            if _pid_alive(job_id):
                out[job_id] = {"state": "RUNNING", "raw": "alive_external"}
            else:
                out[job_id] = {"state": "FAILED", "raw": "missing"}

        return out


def _pid_alive(pid_str: str) -> bool:
    try:
        pid = int(pid_str)
    except (TypeError, ValueError):
        return False

    if pid <= 0:
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True
