from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class TaskState(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class WorkerState(str, Enum):
    STARTING = "STARTING"
    IDLE = "IDLE"
    BUSY = "BUSY"
    DRAINING = "DRAINING"
    OFFLINE = "OFFLINE"


class LaunchState(str, Enum):
    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    ONLINE = "ONLINE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    STALE = "STALE"


@dataclass(slots=True)
class Task:
    task_id: str
    spec_json: str
    state: TaskState
    attempt: int
    max_retries: int
    assigned_worker_id: Optional[str]
    lease_id: Optional[str]
    lease_expires_at_ts: Optional[float]
    result_ref_json: Optional[str]
    error_json: Optional[str]
    created_ts: float
    updated_ts: float
    started_ts: Optional[float]
    finished_ts: Optional[float]


@dataclass(slots=True)
class Worker:
    worker_id: str
    state: WorkerState
    capabilities_json: str
    last_heartbeat_ts: float
    current_lease_id: Optional[str]
    batch_job_id: Optional[str]
    launch_id: Optional[str]


@dataclass(slots=True)
class Lease:
    lease_id: str
    task_id: str
    worker_id: str
    attempt: int
    expires_at_ts: float


@dataclass(slots=True)
class LaunchRecord:
    launch_id: str
    system: str
    batch_job_id: Optional[str]
    submitted_ts: float
    last_update_ts: float
    state: LaunchState
    requested_json: str
    worker_id_expected: Optional[str]


@dataclass(slots=True)
class TaskDefinition:
    task_id: str
    spec: dict
    max_retries: int = 2
