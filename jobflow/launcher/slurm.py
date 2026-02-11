from __future__ import annotations

import logging
import subprocess
import time
import uuid
from typing import Optional

from .base import Launcher
from ..models import LaunchRecord, LaunchState

logger = logging.getLogger(__name__)


def _parse_slurm_job_id(stdout: str) -> Optional[str]:
    # Example: Submitted batch job 12345
    parts = stdout.strip().split()
    if parts and parts[-1].isdigit():
        return parts[-1]
    return None


class SlurmLauncher(Launcher):
    def submit_workers(
        self,
        count: int,
        worker_command: list[str],
        env: dict[str, str],
        requested: dict,
    ) -> list[LaunchRecord]:
        records: list[LaunchRecord] = []
        now = time.time()
        cmd_str = " ".join(worker_command)
        env_opt = ",".join(f"{k}={v}" for k, v in env.items())

        for _ in range(count):
            launch_id = str(uuid.uuid4())
            export_value = "ALL" if not env_opt else f"ALL,{env_opt}"
            sbatch_cmd = ["sbatch", f"--export={export_value}", "--wrap", cmd_str]
            try:
                cp = subprocess.run(sbatch_cmd, check=True, capture_output=True, text=True)
                batch_job_id = _parse_slurm_job_id(cp.stdout)
                state = LaunchState.SUBMITTED
            except Exception:
                logger.exception("Failed submitting SLURM worker")
                batch_job_id = None
                state = LaunchState.FAILED

            records.append(
                LaunchRecord(
                    launch_id=launch_id,
                    system="slurm",
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
        subprocess.run(["scancel", str(batch_job_id)], check=False, capture_output=True, text=True)

    def poll(self, batch_job_ids: list[str]) -> dict[str, dict]:
        if not batch_job_ids:
            return {}

        out: dict[str, dict] = {}
        for job_id in batch_job_ids:
            cp = subprocess.run(
                ["squeue", "-h", "-j", str(job_id), "-o", "%T"],
                check=False,
                capture_output=True,
                text=True,
            )
            status = cp.stdout.strip().upper()

            if not status:
                sacct = subprocess.run(
                    ["sacct", "-j", str(job_id), "--format", "State", "--noheader"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                status = sacct.stdout.strip().split()[0].upper() if sacct.stdout.strip() else "UNKNOWN"

            if status in {"PENDING", "CONFIGURING"}:
                mapped = "PENDING"
            elif status in {"RUNNING", "COMPLETING"}:
                mapped = "RUNNING"
            elif status in {"COMPLETED"}:
                mapped = "RUNNING"
            elif status in {"FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "NODE_FAIL", "OUT_OF_MEMORY"}:
                mapped = "FAILED"
            else:
                mapped = "PENDING"

            out[job_id] = {"state": mapped, "raw": status}
        return out
