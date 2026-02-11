from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Iterable, Tuple

from jobflow import generate_task_id
from jobflow.models import TaskDefinition
from jobflow.program import ProgressCallback, TaskProgram

DEFAULT_BASE_PATH = Path("/omics/groups/OE0441/e230-thrp-data")
DEFAULT_JSON_PATH = DEFAULT_BASE_PATH / "mic_rocket/data/mlarray/id_storage_paths.json"
DEFAULT_DATA_STORAGE_SUBDIR = Path("mic_rocket/data/data_storage")
DEFAULT_OUTPUT_SUBDIR = Path("mic_rocket/data/mlarray/data_store")
DEFAULT_MAX_IMAGES = 1000
DEFAULT_MAX_RETRIES = 2


class DataStorageToMlaProgram(TaskProgram):
    """
    Convert data storage volumes into .mla files.

    The program has built-in defaults and can run with empty --program-args.

    Optional program_args:
    - base_path: base directory used to derive data/output defaults
    - json_path: path to id_storage_paths.json
    - data_storage_base: directory prefix for source files
    - output_dir: directory prefix for output .mla files
    - max_images: optional int limit
    - max_retries: optional per-task retry count
    """

    def generate_tasks(self) -> Iterable[TaskDefinition]:
        base_path = Path(self.program_args.get("base_path", DEFAULT_BASE_PATH)).expanduser().resolve()
        default_data_storage_base = base_path / DEFAULT_DATA_STORAGE_SUBDIR
        default_output_dir = base_path / DEFAULT_OUTPUT_SUBDIR

        json_path = Path(self.program_args.get("json_path", DEFAULT_JSON_PATH)).expanduser().resolve()
        data_storage_base = Path(self.program_args.get("data_storage_base", default_data_storage_base)).expanduser().resolve()
        output_dir = Path(self.program_args.get("output_dir", default_output_dir)).expanduser().resolve()
        max_images = int(self.program_args.get("max_images", DEFAULT_MAX_IMAGES))
        max_retries = int(self.program_args.get("max_retries", DEFAULT_MAX_RETRIES))

        if not json_path.is_file():
            raise FileNotFoundError(f"JSON not found: {json_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        with json_path.open("r", encoding="utf-8") as f:
            image_dicts = json.load(f)

        if not isinstance(image_dicts, list):
            raise ValueError(f"Expected a list in JSON file: {json_path}")

        for image_dict in image_dicts[:max_images]:
            if not isinstance(image_dict, dict):
                continue

            rel = image_dict.get("data_storage_filepath")
            if not isinstance(rel, str) or not rel:
                continue

            rel_path = Path(rel)
            load_path = data_storage_base / rel_path
            save_path = (output_dir / rel_path).with_suffix(".mla")
            if save_path.is_file():
                continue

            task_id = generate_task_id({"program": self.name(), "rel": rel_path.as_posix()})
            yield TaskDefinition(
                task_id=task_id,
                spec={
                    "in": str(load_path),
                    "out": str(save_path),
                    "rel": rel_path.as_posix(),
                },
                max_retries=max_retries,
            )

    def execute_task(self, spec: dict, progress_cb: ProgressCallback) -> dict:
        from medvol import MedVol  # type: ignore
        from mlarray.mlarray import MLArray  # type: ignore

        load_path = Path(spec["in"]).expanduser()
        save_path = Path(spec["out"]).expanduser()

        if save_path.is_file():
            return {"status": "skipped_exists", "path": str(save_path)}

        progress_cb(0.05, {"phase": "load"})
        mv = MedVol(load_path)

        progress_cb(0.35, {"phase": "configure_axes"})
        axis_labels, z_index = axis_order_from_direction(mv.direction)

        mla = MLArray(
            mv.array,
            spacing=mv.spacing,
            origin=mv.origin,
            direction=mv.direction,
            meta=mv.header,
            axis_labels=axis_labels,
        )

        chunk_size = list(mla.shape)
        if len(chunk_size) != 3:
            raise RuntimeError(f"Expected 3D image, got shape={tuple(chunk_size)}")

        block_size = [64, 64, 64]
        chunk_size[z_index] = 1
        block_size[z_index] = 1

        save_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = save_path.parent / f".tmp_{uuid.uuid4().hex}.mla"
        try:
            progress_cb(0.6, {"phase": "write_tmp"})
            mla.save(tmp_path, chunk_size=chunk_size, block_size=block_size)
            progress_cb(0.9, {"phase": "publish"})
            os.replace(tmp_path, save_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        progress_cb(1.0, {"phase": "done"})
        return {
            "status": "ok",
            "path": str(save_path),
            "shape": list(mla.shape),
            "axis_labels": list(axis_labels),
            "z_index": z_index,
        }


def axis_order_from_direction(direction: list[float]) -> Tuple[Tuple[str, str, str], int]:
    """
    Determine axis ordering from a 3x3 ITK direction matrix.

    Returns:
    - ordering: tuple with entries from spatial_x/spatial_y/spatial_z
    - z_index: index of spatial_z in ordering
    """
    import numpy as np  # type: ignore

    direction_arr = np.asarray(direction, dtype=float)
    if direction_arr.shape == (9,):
        direction_arr = direction_arr.reshape(3, 3)
    if direction_arr.shape != (3, 3):
        raise RuntimeError(f"Direction is not 3x3. Got shape={direction_arr.shape}")

    axes = ("spatial_x", "spatial_y", "spatial_z")
    ordering: list[str] = []
    for i in range(3):
        col = direction_arr[:, i]
        world_axis_idx = int(np.argmax(np.abs(col)))
        ordering.append(axes[world_axis_idx])

    ordering_tuple = (ordering[0], ordering[1], ordering[2])
    z_index = ordering_tuple.index("spatial_z")
    return ordering_tuple, z_index
