JobFlow v1 Manager/Worker Coordinator
=====================================

JobFlow provides a programmable distributed task coordinator for SLURM/LSF-style clusters.

Key points:
- Pull-based scheduling: workers request work when idle.
- At-least-once delivery with message deduplication by `msg_id`.
- Lease + heartbeat based failure handling and task requeue.
- Two transport modes:
  - ZeroMQ (`ROUTER` manager, `DEALER` workers) with optional `pyzmq`.
  - Filesystem queue transport over shared storage.
- Delayed worker launch support with separate `LaunchRecord` vs connected `Worker`.

Requirements
------------
- Python 3.10+
- Optional: `pyzmq` for `--mode zmq`
- Optional: `rich` for live manager dashboard (`--dashboard auto|on|off`)
- Install optional dashboard dependency with: `pip install .[dashboard]`

Project modules
---------------
- `jobflow/models.py`
- `jobflow/program.py`
- `jobflow/messages.py`
- `jobflow/transport/base.py`
- `jobflow/transport/zmq_transport.py`
- `jobflow/transport/fs_transport.py`
- `jobflow/store.py`
- `jobflow/scheduler.py`
- `jobflow/launcher/base.py`
- `jobflow/launcher/lsf.py`
- `jobflow/launcher/multiprocess.py`
- `jobflow/launcher/slurm.py`
- `jobflow/manager.py`
- `jobflow/main.py`
- `jobflow/dashboard.py`
- `jobflow/dashboard_subscriber.py`
- `jobflow/telemetry.py`
- `jobflow/worker.py`
- `examples/npy_to_npz_program.py`
  - Demo converts `.npy` inputs to compressed `.npz` outputs.
- `examples/data_storage_to_mla_program.py`
  - Converts data storage volumes into `.mla` outputs (requires `medvol` and `mlarray`).

TaskProgram API
---------------
Implement `jobflow.program.TaskProgram`:
- `generate_tasks()` runs on manager startup once and returns a `list[TaskDefinition]`.
- `execute_task(spec, progress_cb)` runs on workers per assignment.

For compatibility, the manager still accepts legacy iterable/generator task outputs,
but list return is the preferred API.

Load with:
- `--program module.path:ClassName`
- `--program /abs/or/rel/path/to/program.py:ClassName`
- `--program /abs/or/rel/path/to/program.py` (only if file defines exactly one `TaskProgram` class)
- `--program-args '{"key":"value"}'`

Task IDs should be deterministic to keep retries/idempotency consistent.

Additional example program:
- `--program examples.data_storage_to_mla_program:DataStorageToMlaProgram`
- `--program-args '{"json_path":"/path/id_storage_paths.json","data_storage_base":"/path/data_storage","output_dir":"/path/mlarray","max_images":50000}'`

Local demo (filesystem mode)
----------------------------
From repository root:

1. Create demo inputs (optional, if you have numpy):
```bash
python - <<'PY'
from pathlib import Path
import numpy as np
p = Path("/tmp/jobflow_demo/in")
p.mkdir(parents=True, exist_ok=True)
for i in range(8):
    np.save(p / f"arr_{i}.npy", np.arange(1024) + i)
print("created", p)
PY
```

2. Start manager:
```bash
python -m jobflow.manager \
  --mode fs \
  --shared-dir /tmp/jobflow_shared \
  --session-id demo1 \
  --db-path /tmp/jobflow_demo/manager.db \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

3. Start workers (separate shells):
```bash
python -m jobflow.worker \
  --mode fs \
  --shared-dir /tmp/jobflow_shared \
  --session-id demo1 \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

Run multiple workers to process faster.

Local demo (ZeroMQ)
-------------------
Install `pyzmq` first.

1. Start manager:
```bash
python -m jobflow.manager \
  --mode zmq \
  --bind-host 0.0.0.0 \
  --port 5555 \
  --db-path /tmp/jobflow_demo/manager-zmq.db \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

2. Start workers:
```bash
python -m jobflow.worker \
  --mode zmq \
  --manager-host 127.0.0.1 \
  --port 5555 \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

Delayed launches and batch systems
----------------------------------
- Launch requests create `LaunchRecord` rows immediately.
- A launch does not count as online capacity until a worker sends `Register`.
- Long queue delays are expected and safe due to pull-based scheduling.

Optional launcher modules:
- Multiprocess (local): `jobflow.launcher.multiprocess.MultiprocessLauncher`
- LSF: `jobflow.launcher.lsf.LsfLauncher`
- SLURM: `jobflow.launcher.slurm.SlurmLauncher`

Manager can maintain a target worker pool:
- `--launcher {multiprocess|lsf|slurm}`
- `--workers N` (default `10`)
  - Manager keeps this target by submitting replacements when workers go offline.
  - Reconciliation happens only while non-terminal tasks remain.
- LSF launcher options:
  - `--lsf-queue` (default `long`)
  - `--lsf-nproc` (default `10`)
  - `--lsf-mem` (default `20GB`)
  - `--lsf-env-script` (default `~/start_nnunetv2.sh`): prepends `. <script>;` before the worker command in `bash -c`.
- `--shutdown-grace-period SECONDS` for worker shutdown handshake before forced cancel.
- `--worker-manager-timeout-minutes MINUTES` forwarded to launched workers.
- `--dashboard {auto|on|off}` live full-screen manager dashboard (auto enables on TTY).
- `--dashboard-refresh SECONDS` and `--dashboard-log-lines N` tune dashboard updates/log buffer.
- Manager logs are always written live to `<db-path>.log` (next to the SQLite DB).
- `--telemetry-mode {off|file}` enables manager snapshot publishing for external observers.
- `--telemetry-file PATH` output JSON snapshot path (auto-derived when omitted in file mode).
- `--telemetry-queue-size N` bounded publish queue length; oldest snapshots are dropped first.

External dashboard subscriber (separate process)
------------------------------------------------
Use manager telemetry publishing with a separate read-only dashboard process:

1. Start manager with telemetry:
```bash
python -m jobflow.manager \
  --mode fs \
  --shared-dir /tmp/jobflow_shared \
  --session-id demo1 \
  --db-path /tmp/jobflow_demo/manager.db \
  --telemetry-mode file \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

2. Start dashboard subscriber:
```bash
python -m jobflow.dashboard_subscriber \
  --telemetry-file /tmp/jobflow_shared/demo1/dashboard_snapshot.json
```

Supervisor wrapper (phase 3)
----------------------------
Run manager as the authority process and optionally spawn external dashboard automatically:

```bash
jobflow \
  --mode fs \
  --shared-dir /tmp/jobflow_shared \
  --session-id demo1 \
  --db-path /tmp/jobflow_demo/manager.db \
  --program examples.npy_to_npz_program:NpyToNpzProgram \
  --program-args '{"input_dir":"/tmp/jobflow_demo/in","output_dir":"/tmp/jobflow_demo/out","glob":"*.npy"}'
```

Notes:
- `jobflow` now points to the supervisor wrapper and defaults to `--external-dashboard on`.
- Use `jobflow-manager` when you want manager-only behavior.
- Wrapper arguments are parsed first (`--external-dashboard`, `--external-dashboard-refresh`, `--external-dashboard-stale-timeout`, `--supervisor-log-level`).
- All remaining arguments are forwarded to `jobflow.manager`.
- When external dashboard is enabled, wrapper forces manager `--dashboard off` and `--telemetry-mode file`.
- In interactive TTY mode, press `q` in the supervisor terminal to request graceful manager/worker shutdown.

Implementation notes
--------------------
- Message envelope validation is strict (`v`, `msg_id`, `src/dst`, `type`, `payload`).
- Unknown or invalid message direction/types are logged and ignored.
- Dedup persisted in SQLite `processed_messages`.
- Lease expiration triggers requeue until retries are exhausted.
- Manager shutdown flow: detect terminal tasks, send `Shutdown`, wait for `ShutdownAck` or timeout, then cancel remaining launches as fallback.
- Manager restart recovery: assumes workers offline at startup, re-queues in-flight tasks, clears leases, and marks unresolved launches stale.
- Dashboard mode: full-screen view with color progress bar, worker/task stats, and live manager log tail.
