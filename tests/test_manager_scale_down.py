import argparse

from jobflow.manager import Manager
from jobflow.models import LaunchState, WorkerState


class _FakeLauncher:
    def __init__(self) -> None:
        self.canceled: list[str] = []

    def cancel(self, batch_job_id: str) -> None:
        self.canceled.append(str(batch_job_id))


class _FakeStore:
    def __init__(
        self,
        *,
        non_offline_workers: int,
        draining_workers: int,
        pending_launches: int,
        workers: list[dict],
        pending_launch_rows: list[dict],
    ) -> None:
        self._non_offline_workers = non_offline_workers
        self._draining_workers = draining_workers
        self._pending_launches = pending_launches
        self._workers = workers
        self._pending_launch_rows = pending_launch_rows
        self.worker_state_updates: list[tuple[str, WorkerState, str | None]] = []
        self.launch_state_updates: list[tuple[str, LaunchState]] = []

    def count_non_offline_workers(self) -> int:
        return self._non_offline_workers

    def worker_counts(self) -> dict[str, int]:
        return {WorkerState.DRAINING.value: self._draining_workers}

    def count_pending_launches(self) -> int:
        return self._pending_launches

    def list_pending_launches_for_scale_down(self, limit: int) -> list[dict]:
        return self._pending_launch_rows[:limit]

    def mark_launch_state(self, launch_id: str, state: LaunchState) -> None:
        self.launch_state_updates.append((launch_id, state))

    def list_non_offline_workers(self) -> list[dict]:
        return list(self._workers)

    def set_worker_state(self, worker_id: str, state: WorkerState, current_lease_id: str | None = None) -> None:
        self.worker_state_updates.append((worker_id, state, current_lease_id))


def _build_test_manager(store: _FakeStore, launcher: _FakeLauncher, *, target_workers: int) -> tuple[Manager, list[tuple[str, str, dict]], list[int]]:
    manager = Manager.__new__(Manager)
    manager.store = store
    manager.launcher = launcher
    manager.args = argparse.Namespace(workers=target_workers)
    manager._shutdown_started = False
    manager._has_remaining_tasks = lambda: True

    sent_messages: list[tuple[str, str, dict]] = []
    submitted_counts: list[int] = []
    manager._send_to_worker = lambda worker_id, msg_type, payload: sent_messages.append((worker_id, msg_type, payload))
    manager._submit_workers = lambda count: submitted_counts.append(count)
    return manager, sent_messages, submitted_counts


def test_reconcile_scales_down_idle_first_and_skips_draining_workers() -> None:
    store = _FakeStore(
        non_offline_workers=12,
        draining_workers=2,
        pending_launches=0,
        workers=[
            {"worker_id": "w-idle", "state": WorkerState.IDLE.value, "current_lease_id": None},
            {"worker_id": "w-busy", "state": WorkerState.BUSY.value, "current_lease_id": "lease-1"},
            {"worker_id": "w-draining", "state": WorkerState.DRAINING.value, "current_lease_id": None},
        ],
        pending_launch_rows=[],
    )
    launcher = _FakeLauncher()
    manager, sent_messages, submitted_counts = _build_test_manager(store, launcher, target_workers=9)

    manager._reconcile_worker_pool()

    assert submitted_counts == []
    assert launcher.canceled == []
    assert store.worker_state_updates == [("w-idle", WorkerState.DRAINING, None)]
    assert sent_messages == [("w-idle", "Shutdown", {"graceful": True})]


def test_reconcile_cancels_pending_launches_before_draining_workers() -> None:
    store = _FakeStore(
        non_offline_workers=8,
        draining_workers=0,
        pending_launches=2,
        workers=[
            {"worker_id": "w-idle", "state": WorkerState.IDLE.value, "current_lease_id": None},
        ],
        pending_launch_rows=[
            {"launch_id": "l1", "batch_job_id": "1001"},
            {"launch_id": "l2", "batch_job_id": "1002"},
        ],
    )
    launcher = _FakeLauncher()
    manager, sent_messages, submitted_counts = _build_test_manager(store, launcher, target_workers=8)

    manager._reconcile_worker_pool()

    assert submitted_counts == []
    assert launcher.canceled == ["1001", "1002"]
    assert store.launch_state_updates == [("l1", LaunchState.CANCELED), ("l2", LaunchState.CANCELED)]
    assert store.worker_state_updates == []
    assert sent_messages == []

