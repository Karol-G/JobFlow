from pathlib import Path

from jobflow.models import LaunchState, TaskDefinition, TaskState, WorkerState
from jobflow.store import Store


def test_recover_after_manager_restart_resets_transient_state(tmp_path: Path) -> None:
    db_path = tmp_path / "manager.db"
    store = Store(db_path)
    try:
        # Seed tasks and mark two as in-flight.
        store.bulk_upsert_tasks(
            [
                TaskDefinition(task_id="t1", spec={"x": 1}, max_retries=2),
                TaskDefinition(task_id="t2", spec={"x": 2}, max_retries=2),
                TaskDefinition(task_id="t3", spec={"x": 3}, max_retries=2),
            ]
        )
        lease_t1 = store.create_lease("t1", "w1", attempt=0, expires_at_ts=10_000)
        lease_t2 = store.create_lease("t2", "w2", attempt=0, expires_at_ts=10_000)
        store.set_task_state("t1", TaskState.ASSIGNED, assigned_worker_id="w1", lease_id=lease_t1, lease_expires_at_ts=10_000)
        store.set_task_state("t2", TaskState.RUNNING, assigned_worker_id="w2", lease_id=lease_t2, lease_expires_at_ts=10_000)
        store.set_task_state("t3", TaskState.SUCCEEDED)

        # Seed workers with non-offline states and current leases.
        now = 1_234.0
        store.upsert_worker("w1", state=WorkerState.BUSY, capabilities={}, last_heartbeat_ts=now, current_lease_id=lease_t1)
        store.upsert_worker("w2", state=WorkerState.IDLE, capabilities={}, last_heartbeat_ts=now, current_lease_id=lease_t2)
        store.upsert_worker("w3", state=WorkerState.OFFLINE, capabilities={}, last_heartbeat_ts=now, current_lease_id=None)

        # Seed launches with transient and terminal states.
        store.upsert_launch("l1", system="test", state=LaunchState.SUBMITTED, requested={})
        store.upsert_launch("l2", system="test", state=LaunchState.RUNNING, requested={})
        store.upsert_launch("l3", system="test", state=LaunchState.ONLINE, requested={})
        store.upsert_launch("l4", system="test", state=LaunchState.FAILED, requested={})

        counts = store.recover_after_manager_restart(now_ts=2_000.0)
        assert counts == {
            "workers_offlined": 2,
            "tasks_requeued": 2,
            "leases_cleared": 2,
            "launches_staled": 3,
        }

        # Workers are immediately offline.
        worker_counts = store.worker_counts()
        assert worker_counts == {WorkerState.OFFLINE.value: 3}

        # In-flight tasks are queued again; terminal task remains terminal.
        assert store.get_task("t1")["state"] == TaskState.QUEUED.value
        assert store.get_task("t2")["state"] == TaskState.QUEUED.value
        assert store.get_task("t3")["state"] == TaskState.SUCCEEDED.value

        # Leases are gone.
        assert store.expire_leases(9_999_999) == []

        # Transient launches are stale; terminal launch unchanged.
        assert store.find_launch("l1")["state"] == LaunchState.STALE.value
        assert store.find_launch("l2")["state"] == LaunchState.STALE.value
        assert store.find_launch("l3")["state"] == LaunchState.STALE.value
        assert store.find_launch("l4")["state"] == LaunchState.FAILED.value
    finally:
        store.close()
