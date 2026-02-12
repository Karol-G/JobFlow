from pathlib import Path

from jobflow.models import TaskDefinition, TaskState
from jobflow.store import Store


def test_bulk_upsert_tasks_basic_and_existing(tmp_path: Path) -> None:
    db_path = tmp_path / "manager.db"
    store = Store(db_path)
    try:
        tasks = [
            TaskDefinition(task_id="t1", spec={"x": 1}, max_retries=2),
            TaskDefinition(task_id="t2", spec={"x": 2}, max_retries=2),
        ]
        first = store.bulk_upsert_tasks(tasks)
        assert first == {
            "inserted": 2,
            "exists": 0,
            "spec_mismatch": 0,
            "terminal_exists": 0,
        }

        second = store.bulk_upsert_tasks(tasks)
        assert second == {
            "inserted": 0,
            "exists": 2,
            "spec_mismatch": 0,
            "terminal_exists": 0,
        }
    finally:
        store.close()


def test_bulk_upsert_tasks_mismatch_terminal_and_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "manager.db"
    store = Store(db_path)
    try:
        base = [
            TaskDefinition(task_id="t1", spec={"x": 1}, max_retries=2),
            TaskDefinition(task_id="t2", spec={"x": 2}, max_retries=2),
        ]
        store.bulk_upsert_tasks(base)
        store.set_task_state("t1", TaskState.SUCCEEDED)

        mixed = [
            TaskDefinition(task_id="t1", spec={"x": 1}, max_retries=2),  # terminal_exists
            TaskDefinition(task_id="t2", spec={"x": 999}, max_retries=2),  # spec_mismatch
            TaskDefinition(task_id="t3", spec={"x": 3}, max_retries=2),  # inserted
            TaskDefinition(task_id="t3", spec={"x": 3}, max_retries=2),  # exists (duplicate in same batch)
            TaskDefinition(task_id="t3", spec={"x": 4}, max_retries=2),  # spec_mismatch (duplicate in same batch)
        ]
        counts = store.bulk_upsert_tasks(mixed)
        assert counts == {
            "inserted": 1,
            "exists": 1,
            "spec_mismatch": 2,
            "terminal_exists": 1,
        }
    finally:
        store.close()
