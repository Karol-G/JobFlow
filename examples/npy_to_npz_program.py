from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import Iterable

# Allow direct script execution: `python examples/npy_to_npz_program.py`
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobflow.models import TaskDefinition
from jobflow.program import ProgressCallback, TaskProgram
from jobflow.manager import Manager
from jobflow import generate_task_id


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
            task_id = generate_task_id(rel)
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


def _run_demo() -> int:
    try:
        import numpy as np  # noqa: F401  # type: ignore
    except Exception:
        print("This demo requires numpy installed.", file=sys.stderr)
        return 2

    tmp_root = Path.cwd() / f"jobflow_npz_demo_{uuid.uuid4().hex[:10]}"
    tmp_root.mkdir(parents=True, exist_ok=False)
    try:
        input_dir = tmp_root / "input"
        output_dir = tmp_root / "output"
        shared_dir = tmp_root / "shared"
        db_path = tmp_root / "manager.db"
        session_id = f"demo-{uuid.uuid4().hex[:8]}"

        num_tasks = _create_demo_arrays(input_dir, count=60, seed=42)
        print(f"Created {num_tasks} input arrays in {input_dir}")

        program_args = {
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "glob": "*.npy",
        }
        manager_args = argparse.Namespace(
            mode="fs",
            bind_host="0.0.0.0",
            port=5555,
            manager_host=None,
            shared_dir=str(shared_dir),
            session_id=session_id,
            lease_duration=60,
            heartbeat_interval=2,
            worker_timeout=20,
            db_path=str(db_path),
            program="npy_to_npz_program:NpyToNpzProgram",
            program_args=json.dumps(program_args, sort_keys=True),
            enable_launcher="multiprocess",
            launch_stale_timeout=21600,
            launch_poll_interval=60,
            worker_count_on_start=3,
            shutdown_grace_period=20,
            worker_manager_timeout_minutes=1.0,
            log_level="INFO",
            dashboard="on"
        )

        manager = Manager(manager_args)
        print("Starting manager with multiprocess launcher (in-process)...")
        try:
            summary = manager.run()
            succeeded = summary.get("SUCCEEDED", 0)
            if succeeded != num_tasks:
                raise RuntimeError(f"Expected {num_tasks} succeeded tasks, got summary={summary}")
            out_files = sorted(output_dir.rglob("*.npz"))
            print(f"Task summary: {summary}")
            print(f"Output files: {len(out_files)}")
            for p in out_files[:3]:
                print(f"  {p}")
            return_code = 0
        except Exception as exc:
            print(f"Demo failed: {exc}", file=sys.stderr)
            return_code = 1

        return return_code
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
        print(f"Deleted demo directory: {tmp_root}")


def main() -> int:
    return _run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
