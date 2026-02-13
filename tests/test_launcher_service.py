import time

from jobflow.launcher.service import PRIORITY_CANCEL, PRIORITY_POLL, LauncherService
from jobflow.models import LaunchRecord, LaunchState


class _FakeLauncher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def submit_workers(self, count: int, worker_command: list[str], env: dict[str, str], requested: dict) -> list[LaunchRecord]:
        self.calls.append(("submit", str(count)))
        now = time.time()
        return [
            LaunchRecord(
                launch_id="l1",
                system="fake",
                batch_job_id="b1",
                submitted_ts=now,
                last_update_ts=now,
                state=LaunchState.SUBMITTED,
                requested_json="{}",
                worker_id_expected=None,
            )
        ]

    def cancel(self, batch_job_id: str) -> None:
        self.calls.append(("cancel", str(batch_job_id)))

    def poll(self, batch_job_ids: list[str]) -> dict[str, dict]:
        self.calls.append(("poll", ",".join(batch_job_ids)))
        time.sleep(0.2)
        return {job_id: {"state": "RUNNING", "raw": "RUN"} for job_id in batch_job_ids}


def _wait_for_results(service: LauncherService, expected: int, timeout_s: float = 3.0):
    deadline = time.time() + timeout_s
    out = []
    while time.time() < deadline and len(out) < expected:
        out.extend(service.drain_results(max_items=32))
        if len(out) >= expected:
            break
        time.sleep(0.02)
    return out


def test_launcher_service_priority_cancel_before_poll() -> None:
    launcher = _FakeLauncher()
    service = LauncherService(launcher, command_queue_size=16, result_queue_size=16)
    try:
        # Enqueue before start to make processing order deterministic.
        service.enqueue(kind="poll", payload={"batch_job_ids": ["10"]}, priority=PRIORITY_POLL, timeout_s=1.0, max_retries=0)
        service.enqueue(kind="cancel", payload={"batch_job_id": "10"}, priority=PRIORITY_CANCEL, timeout_s=1.0, max_retries=0)
        service.start()
        _wait_for_results(service, expected=2)
    finally:
        service.stop(timeout_s=2.0)

    assert len(launcher.calls) >= 2
    assert launcher.calls[0][0] == "cancel"


def test_launcher_service_timeout_and_retry() -> None:
    launcher = _FakeLauncher()
    service = LauncherService(launcher, command_queue_size=16, result_queue_size=16)
    try:
        service.start()
        service.enqueue(kind="poll", payload={"batch_job_ids": ["20"]}, priority=PRIORITY_POLL, timeout_s=0.05, max_retries=1)
        results = _wait_for_results(service, expected=1)
    finally:
        service.stop(timeout_s=2.0)

    assert len(results) == 1
    result = results[0]
    assert result.kind == "poll"
    assert result.ok is False
    assert result.timed_out is True
    assert result.attempts == 2
