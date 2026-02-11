from __future__ import annotations

import json
import time
import uuid
from typing import Optional

SUPPORTED_VERSION = 1

WORKER_TO_MANAGER_TYPES = {
    "Register",
    "Heartbeat",
    "RequestWork",
    "TaskStarted",
    "TaskProgress",
    "TaskFinished",
    "TaskFailed",
    "ShutdownAck",
}

MANAGER_TO_WORKER_TYPES = {
    "Welcome",
    "Assign",
    "Cancel",
    "Shutdown",
}

ALL_TYPES = WORKER_TO_MANAGER_TYPES | MANAGER_TO_WORKER_TYPES


class MessageValidationError(ValueError):
    pass


def now_ts() -> float:
    return time.time()


def new_msg_id() -> str:
    return str(uuid.uuid4())


def make_envelope(
    *,
    src_role: str,
    src_id: str,
    dst_role: str,
    dst_id: str,
    msg_type: str,
    payload: dict,
    msg_id: Optional[str] = None,
    ts: Optional[float] = None,
) -> dict:
    msg = {
        "v": SUPPORTED_VERSION,
        "msg_id": msg_id or new_msg_id(),
        "ts": ts or now_ts(),
        "src": {"role": src_role, "id": src_id},
        "dst": {"role": dst_role, "id": dst_id},
        "type": msg_type,
        "payload": payload,
    }
    validate_envelope(msg)
    return msg


def validate_envelope(msg: dict) -> None:
    if not isinstance(msg, dict):
        raise MessageValidationError("Message must be a dict.")

    required_top = {"v", "msg_id", "ts", "src", "dst", "type", "payload"}
    missing = required_top - set(msg)
    if missing:
        raise MessageValidationError(f"Missing envelope fields: {sorted(missing)}")

    if msg["v"] != SUPPORTED_VERSION:
        raise MessageValidationError(f"Unsupported message version: {msg['v']}")
    if not isinstance(msg["msg_id"], str) or not msg["msg_id"]:
        raise MessageValidationError("msg_id must be a non-empty string")
    if not isinstance(msg["ts"], (int, float)):
        raise MessageValidationError("ts must be numeric")

    for side in ("src", "dst"):
        part = msg[side]
        if not isinstance(part, dict):
            raise MessageValidationError(f"{side} must be a dict")
        for key in ("role", "id"):
            if key not in part:
                raise MessageValidationError(f"Missing {side}.{key}")
            if not isinstance(part[key], str) or not part[key]:
                raise MessageValidationError(f"{side}.{key} must be non-empty string")

    if not isinstance(msg["type"], str) or not msg["type"]:
        raise MessageValidationError("type must be a non-empty string")
    if not isinstance(msg["payload"], dict):
        raise MessageValidationError("payload must be a dict")


def validate_type_for_direction(msg: dict) -> bool:
    msg_type = msg["type"]
    src_role = msg["src"]["role"]
    dst_role = msg["dst"]["role"]

    if src_role == "worker" and dst_role == "manager":
        return msg_type in WORKER_TO_MANAGER_TYPES
    if src_role == "manager" and dst_role == "worker":
        return msg_type in MANAGER_TO_WORKER_TYPES
    return False


def encode_message(msg: dict) -> bytes:
    validate_envelope(msg)
    return json.dumps(msg, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode_message(raw: bytes) -> dict:
    msg = json.loads(raw.decode("utf-8"))
    validate_envelope(msg)
    return msg
