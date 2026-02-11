from __future__ import annotations

import logging
import subprocess
import time
import uuid
from typing import Optional

from .base import Launcher
from ..models import LaunchRecord, LaunchState

logger = logging.getLogger(__name__)


def _parse_lsf_job_id(stdout: str) -> Optional[str]:
    # Example: Job <12345> is submitted to queue <normal>.
    start = stdout.find("<")
    end = stdout.find(">", start + 1)
    if start >= 0 and end > start:
        return stdout[start + 1 : end]
    return None


class LsfLauncher(Launcher):
    def submit_workers(
        self,
        count: int,
        worker_command: list[str],
        env: dict[str, str],
        requested: dict,
    ) -> list[LaunchRecord]:
        records: list[LaunchRecord] = []
        now = time.time()
        queue = str(requested.get("lsf_queue", "long"))
        nproc = int(requested.get("lsf_nproc", 10))
        mem = str(requested.get("lsf_mem", "20GB"))
        env_script = str(requested.get("lsf_env_script", "")).strip()

        for _ in range(count):
            launch_id = str(uuid.uuid4())
            env_pairs = ",".join(f"{k}={v}" for k, v in env.items())
            env_value = "all" if not env_pairs else f"all,{env_pairs}"
            # Use bash positional parameters so worker_command arguments are not shell-reparsed.
            # This avoids JSON quoting issues for --program-args in LSF command wrapping.
            bash_script = 'if [ -n "$1" ]; then . "$1"; shift; fi; exec "$@"'
            bsub_cmd = [
                "bsub",
                "-q",
                queue,
                "-n",
                str(nproc),
                "-R",
                f"rusage[mem={mem}]",
                "-env",
                env_value,
                "/bin/bash",
                "-l",
                "-c",
                bash_script,
                "jobflow-worker",
                env_script,
                *worker_command,
            ]
            logger.debug("Submitting LSF worker command: %s", bsub_cmd)
            try:
                cp = subprocess.run(bsub_cmd, check=True, capture_output=True, text=True)
                batch_job_id = _parse_lsf_job_id(cp.stdout)
                state = LaunchState.SUBMITTED
            except Exception:
                logger.exception("Failed submitting LSF worker")
                batch_job_id = None
                state = LaunchState.FAILED

            records.append(
                LaunchRecord(
                    launch_id=launch_id,
                    system="lsf",
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
        subprocess.run(["bkill", str(batch_job_id)], check=False, capture_output=True, text=True)

    def poll(self, batch_job_ids: list[str]) -> dict[str, dict]:
        if not batch_job_ids:
            return {}

        out: dict[str, dict] = {}
        for job_id in batch_job_ids:
            cp = subprocess.run(["bjobs", "-o", "stat", "-noheader", str(job_id)], check=False, capture_output=True, text=True)
            status = cp.stdout.strip().upper()
            if status in {"PEND", "PSUSP", "USUSP", "SSUSP"}:
                mapped = "PENDING"
            elif status in {"RUN"}:
                mapped = "RUNNING"
            elif status in {"DONE"}:
                mapped = "RUNNING"
            elif status in {"EXIT", "ZOMBI", "UNKWN"}:
                mapped = "FAILED"
            else:
                mapped = "PENDING"
            out[job_id] = {"state": mapped, "raw": status}
        return out
