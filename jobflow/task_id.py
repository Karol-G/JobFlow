from __future__ import annotations

import hashlib
import json
from typing import Any


def generate_task_id(value: Any) -> str:
    """
    Generate a deterministic task ID from JSON-serializable input.
    """
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()
