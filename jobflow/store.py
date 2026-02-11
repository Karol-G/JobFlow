from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional

from .models import LaunchState, TaskState, WorkerState

logger = logging.getLogger(__name__)

TERMINAL_TASK_STATES = {TaskState.SUCCEEDED.value, TaskState.FAILED.value, TaskState.CANCELED.value}


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                spec_json TEXT NOT NULL,
                state TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                assigned_worker_id TEXT,
                lease_id TEXT,
                lease_expires_at_ts REAL,
                result_ref_json TEXT,
                error_json TEXT,
                created_ts REAL NOT NULL,
                updated_ts REAL NOT NULL,
                started_ts REAL,
                finished_ts REAL
            );

            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                last_heartbeat_ts REAL NOT NULL,
                current_lease_id TEXT,
                batch_job_id TEXT,
                launch_id TEXT
            );

            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                expires_at_ts REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS launches (
                launch_id TEXT PRIMARY KEY,
                system TEXT NOT NULL,
                batch_job_id TEXT,
                submitted_ts REAL NOT NULL,
                last_update_ts REAL NOT NULL,
                state TEXT NOT NULL,
                requested_json TEXT NOT NULL,
                worker_id_expected TEXT
            );

            CREATE TABLE IF NOT EXISTS processed_messages (
                msg_id TEXT PRIMARY KEY,
                ts REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_state_updated ON tasks(state, updated_ts);
            CREATE INDEX IF NOT EXISTS idx_workers_state ON workers(state);
            CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at_ts);
            CREATE INDEX IF NOT EXISTS idx_launches_batch_job_id ON launches(batch_job_id);
            """
        )
        self.conn.commit()

    def dedup_check_and_record(self, msg_id: str, ts: Optional[float] = None) -> bool:
        now = ts or time.time()
        try:
            self.conn.execute("INSERT INTO processed_messages(msg_id, ts) VALUES (?, ?)", (msg_id, now))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def upsert_task(self, task_id: str, spec_dict: dict, max_retries: int) -> str:
        now = time.time()
        spec_json = json.dumps(spec_dict, sort_keys=True, separators=(",", ":"))

        row = self.conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            self.conn.execute(
                """
                INSERT INTO tasks(
                    task_id, spec_json, state, attempt, max_retries,
                    assigned_worker_id, lease_id, lease_expires_at_ts,
                    result_ref_json, error_json,
                    created_ts, updated_ts, started_ts, finished_ts
                ) VALUES (?, ?, ?, 0, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL)
                """,
                (task_id, spec_json, TaskState.QUEUED.value, max_retries, now, now),
            )
            self.conn.commit()
            return "inserted"

        if row["state"] in TERMINAL_TASK_STATES:
            return "terminal_exists"

        if row["spec_json"] != spec_json:
            return "spec_mismatch"

        self.conn.execute(
            "UPDATE tasks SET spec_json = ?, max_retries = ?, updated_ts = ? WHERE task_id = ?",
            (spec_json, max_retries, now, task_id),
        )
        self.conn.commit()
        return "exists"

    def get_task(self, task_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()

    def get_next_queued_task(self) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE state = ?
            ORDER BY updated_ts ASC, created_ts ASC
            LIMIT 1
            """,
            (TaskState.QUEUED.value,),
        ).fetchone()

    def task_counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) as c FROM tasks GROUP BY state").fetchall()
        return {row["state"]: row["c"] for row in rows}

    def worker_counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) as c FROM workers GROUP BY state").fetchall()
        return {row["state"]: row["c"] for row in rows}

    def set_task_state(
        self,
        task_id: str,
        state: TaskState,
        *,
        attempt: Optional[int] = None,
        assigned_worker_id: Optional[str] = None,
        lease_id: Optional[str] = None,
        lease_expires_at_ts: Optional[float] = None,
        result_ref: Optional[dict] = None,
        error: Optional[dict] = None,
        started_ts: Optional[float] = None,
        finished_ts: Optional[float] = None,
    ) -> None:
        now = time.time()
        self.conn.execute(
            """
            UPDATE tasks
            SET
                state = ?,
                attempt = COALESCE(?, attempt),
                assigned_worker_id = ?,
                lease_id = ?,
                lease_expires_at_ts = ?,
                result_ref_json = COALESCE(?, result_ref_json),
                error_json = COALESCE(?, error_json),
                started_ts = COALESCE(?, started_ts),
                finished_ts = COALESCE(?, finished_ts),
                updated_ts = ?
            WHERE task_id = ?
            """,
            (
                state.value,
                attempt,
                assigned_worker_id,
                lease_id,
                lease_expires_at_ts,
                json.dumps(result_ref, sort_keys=True, separators=(",", ":")) if result_ref is not None else None,
                json.dumps(error, sort_keys=True, separators=(",", ":")) if error is not None else None,
                started_ts,
                finished_ts,
                now,
                task_id,
            ),
        )
        self.conn.commit()

    def create_lease(self, task_id: str, worker_id: str, attempt: int, expires_at_ts: float) -> str:
        lease_id = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO leases(lease_id, task_id, worker_id, attempt, expires_at_ts) VALUES (?, ?, ?, ?, ?)",
            (lease_id, task_id, worker_id, attempt, expires_at_ts),
        )
        self.conn.commit()
        return lease_id

    def remove_lease(self, lease_id: str) -> None:
        self.conn.execute("DELETE FROM leases WHERE lease_id = ?", (lease_id,))
        self.conn.commit()

    def get_lease(self, lease_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()

    def expire_leases(self, now_ts: float) -> list[tuple[str, str, str, int]]:
        rows = self.conn.execute(
            "SELECT task_id, lease_id, worker_id, attempt FROM leases WHERE expires_at_ts <= ?",
            (now_ts,),
        ).fetchall()
        if not rows:
            return []

        self.conn.execute("DELETE FROM leases WHERE expires_at_ts <= ?", (now_ts,))
        self.conn.commit()
        return [(r["task_id"], r["lease_id"], r["worker_id"], r["attempt"]) for r in rows]

    def upsert_worker(
        self,
        worker_id: str,
        *,
        state: WorkerState,
        capabilities: dict,
        last_heartbeat_ts: float,
        current_lease_id: Optional[str] = None,
        batch_job_id: Optional[str] = None,
        launch_id: Optional[str] = None,
    ) -> None:
        capabilities_json = json.dumps(capabilities, sort_keys=True, separators=(",", ":"))
        self.conn.execute(
            """
            INSERT INTO workers(worker_id, state, capabilities_json, last_heartbeat_ts, current_lease_id, batch_job_id, launch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
              state = excluded.state,
              capabilities_json = excluded.capabilities_json,
              last_heartbeat_ts = excluded.last_heartbeat_ts,
              current_lease_id = excluded.current_lease_id,
              batch_job_id = COALESCE(excluded.batch_job_id, workers.batch_job_id),
              launch_id = COALESCE(excluded.launch_id, workers.launch_id)
            """,
            (worker_id, state.value, capabilities_json, last_heartbeat_ts, current_lease_id, batch_job_id, launch_id),
        )
        self.conn.commit()

    def update_worker_heartbeat(self, worker_id: str, ts: float, current_lease_id: Optional[str]) -> None:
        self.conn.execute(
            """
            UPDATE workers SET
                last_heartbeat_ts = ?,
                current_lease_id = COALESCE(?, current_lease_id),
                state = CASE WHEN state = ? THEN ? ELSE state END
            WHERE worker_id = ?
            """,
            (ts, current_lease_id, WorkerState.OFFLINE.value, WorkerState.IDLE.value, worker_id),
        )
        self.conn.commit()

    def set_worker_state(self, worker_id: str, state: WorkerState, current_lease_id: Optional[str] = None) -> None:
        self.conn.execute(
            "UPDATE workers SET state = ?, current_lease_id = ? WHERE worker_id = ?",
            (state.value, current_lease_id, worker_id),
        )
        self.conn.commit()

    def mark_worker_offline(self, worker_id: str) -> None:
        self.conn.execute(
            "UPDATE workers SET state = ?, current_lease_id = NULL WHERE worker_id = ?",
            (WorkerState.OFFLINE.value, worker_id),
        )
        self.conn.commit()

    def workers_last_seen_before(self, ts_cutoff: float) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM workers WHERE last_heartbeat_ts < ? AND state != ?",
            (ts_cutoff, WorkerState.OFFLINE.value),
        ).fetchall()

    def get_worker(self, worker_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()

    def list_non_offline_workers(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM workers WHERE state != ?",
            (WorkerState.OFFLINE.value,),
        ).fetchall()

    def upsert_launch(
        self,
        launch_id: str,
        *,
        system: str,
        state: LaunchState,
        requested: dict,
        batch_job_id: Optional[str] = None,
        worker_id_expected: Optional[str] = None,
    ) -> None:
        now = time.time()
        requested_json = json.dumps(requested, sort_keys=True, separators=(",", ":"))
        self.conn.execute(
            """
            INSERT INTO launches(launch_id, system, batch_job_id, submitted_ts, last_update_ts, state, requested_json, worker_id_expected)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(launch_id) DO UPDATE SET
                system = excluded.system,
                batch_job_id = COALESCE(excluded.batch_job_id, launches.batch_job_id),
                last_update_ts = excluded.last_update_ts,
                state = excluded.state,
                requested_json = excluded.requested_json,
                worker_id_expected = COALESCE(excluded.worker_id_expected, launches.worker_id_expected)
            """,
            (launch_id, system, batch_job_id, now, now, state.value, requested_json, worker_id_expected),
        )
        self.conn.commit()

    def mark_launch_state(self, launch_id: str, state: LaunchState) -> None:
        now = time.time()
        self.conn.execute(
            "UPDATE launches SET state = ?, last_update_ts = ? WHERE launch_id = ?",
            (state.value, now, launch_id),
        )
        self.conn.commit()

    def update_launch_batch_job_id(self, launch_id: str, batch_job_id: str) -> None:
        now = time.time()
        self.conn.execute(
            "UPDATE launches SET batch_job_id = ?, last_update_ts = ? WHERE launch_id = ?",
            (batch_job_id, now, launch_id),
        )
        self.conn.commit()

    def find_launch_by_batch_job_id(self, job_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM launches WHERE batch_job_id = ?", (job_id,)).fetchone()

    def find_launch(self, launch_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM launches WHERE launch_id = ?", (launch_id,)).fetchone()

    def list_unresolved_launches(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM launches WHERE state NOT IN (?, ?, ?, ?)",
            (
                LaunchState.ONLINE.value,
                LaunchState.FAILED.value,
                LaunchState.CANCELED.value,
                LaunchState.STALE.value,
            ),
        ).fetchall()

    def list_launches_for_poll(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM launches WHERE batch_job_id IS NOT NULL AND state NOT IN (?, ?, ?)",
            (LaunchState.ONLINE.value, LaunchState.FAILED.value, LaunchState.CANCELED.value),
        ).fetchall()

    def list_launches_for_cancel(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM launches WHERE batch_job_id IS NOT NULL AND state NOT IN (?, ?)",
            (LaunchState.FAILED.value, LaunchState.CANCELED.value),
        ).fetchall()

    def launch_counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) as c FROM launches GROUP BY state").fetchall()
        return {row["state"]: row["c"] for row in rows}
