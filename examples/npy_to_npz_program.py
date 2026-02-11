from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Iterable

# Allow direct script execution: `python examples/npy_to_npz_program.py`
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobflow.models import TaskDefinition
from jobflow.program import ProgressCallback, TaskProgram


class NpyToNpzProgram(TaskProgram):
    """
    Demo workload that converts .npy arrays into compressed .npz files.

    The class name is kept for compatibility with existing references.
    """

    def generate_tasks(self) -> Iterable[TaskDefinition]:
        input_dir = Path(self.program_args["input_dir"]).resolve()
        output_dir = Path(self.program_args["output_dir"]).resolve()
        glob_pat = self.program_args.get("glob", "*.npy")

        for in_path in sorted(input_dir.rglob(glob_pat)):
            if not in_path.is_file():
                continue
            rel = in_path.relative_to(input_dir).as_posix()
            task_id = hashlib.sha1(rel.encode("utf-8")).hexdigest()
            out_path = (output_dir / in_path.relative_to(input_dir)).with_suffix(".npz")
            yield TaskDefinition(
                task_id=task_id,
                spec={
                    "in": str(in_path),
                    "out": str(out_path),
                    "rel": rel,
                },
                max_retries=2,
            )

    def execute_task(self, spec: dict, progress_cb: ProgressCallback) -> dict:
        import numpy as np  # type: ignore

        in_path = Path(spec["in"])
        out_path = Path(spec["out"])
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if out_path.exists():
            return {
                "status": "skipped_exists",
                "path": str(out_path),
                "size": out_path.stat().st_size,
            }

        progress_cb(0.1, {"phase": "load_npy"})
        arr = np.load(in_path, allow_pickle=False)

        tmp_path = out_path.parent / f".tmp_{uuid.uuid4().hex}.npz"
        try:
            with tmp_path.open("wb") as fh:
                np.savez_compressed(fh, array=arr)
                fh.flush()
                os.fsync(fh.fileno())
            progress_cb(0.8, {"phase": "write_npz"})
            os.replace(tmp_path, out_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        progress_cb(1.0, {"phase": "done"})
        return {
            "status": "ok",
            "path": str(out_path),
            "size": out_path.stat().st_size,
            "shape": list(arr.shape),
            "dtype": str(arr.dtype),
        }


def _create_demo_arrays(input_dir: Path, count: int, seed: int) -> int:
    import numpy as np  # type: ignore

    rng = np.random.default_rng(seed)
    input_dir.mkdir(parents=True, exist_ok=True)

    for i in range(count):
        arr = rng.normal(loc=0.0, scale=1.0, size=(128 + i, 64)).astype("float32")
        np.save(input_dir / f"array_{i:03d}.npy", arr)
    return count


def _wait_for_terminal_tasks(db_path: Path, expected_tasks: int, timeout_s: float) -> dict[str, int]:
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        if not db_path.exists():
            time.sleep(0.2)
            continue

        conn = sqlite3.connect(str(db_path))
        try:
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            terminal = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE state IN ('SUCCEEDED', 'FAILED', 'CANCELED')"
            ).fetchone()[0]
            if total >= expected_tasks and terminal == total:
                rows = conn.execute("SELECT state, COUNT(*) FROM tasks GROUP BY state").fetchall()
                return {state: count for state, count in rows}
        finally:
            conn.close()

        time.sleep(0.3)

    raise TimeoutError(f"Timed out waiting for {expected_tasks} tasks to finish")


def _run_demo() -> int:
    try:
        import numpy as np  # noqa: F401  # type: ignore
    except Exception:
        print("This demo requires numpy installed.", file=sys.stderr)
        return 2

    repo_root = Path(__file__).resolve().parents[1]

    with tempfile.TemporaryDirectory(prefix="jobflow_npz_demo_") as tmp_root_raw:
        tmp_root = Path(tmp_root_raw)
        input_dir = tmp_root / "input"
        output_dir = tmp_root / "output"
        shared_dir = tmp_root / "shared"
        db_path = tmp_root / "manager.db"
        session_id = f"demo-{uuid.uuid4().hex[:8]}"

        num_tasks = _create_demo_arrays(input_dir, count=6, seed=42)
        print(f"Created {num_tasks} input arrays in {input_dir}")

        program_args = {
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "glob": "*.npy",
        }
        manager_cmd = [
            sys.executable,
            "-m",
            "jobflow.manager",
            "--mode",
            "fs",
            "--shared-dir",
            str(shared_dir),
            "--session-id",
            session_id,
            "--db-path",
            str(db_path),
            "--program",
            "examples.npy_to_npz_program:NpyToNpzProgram",
            "--program-args",
            json.dumps(program_args, sort_keys=True),
            "--enable-launcher",
            "multiprocess",
            "--worker-count-on-start",
            "3",
            "--heartbeat-interval",
            "2",
            "--worker-timeout",
            "20",
            "--lease-duration",
            "60",
            "--log-level",
            "INFO",
        ]

        print("Starting manager with multiprocess launcher...")
        manager_proc = subprocess.Popen(manager_cmd, cwd=str(repo_root), start_new_session=True)

        try:
            summary = _wait_for_terminal_tasks(db_path, expected_tasks=num_tasks, timeout_s=120.0)
            out_files = sorted(output_dir.rglob("*.npz"))
            print(f"Task summary: {summary}")
            print(f"Output files: {len(out_files)}")
            for p in out_files[:3]:
                print(f"  {p}")
            return_code = 0
        except Exception as exc:
            print(f"Demo failed: {exc}", file=sys.stderr)
            return_code = 1
        finally:
            try:
                os.killpg(manager_proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                manager_proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(manager_proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                manager_proc.wait(timeout=5.0)

        print(f"Temporary files cleaned up from {tmp_root}")
        return return_code


def main() -> int:
    return _run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
