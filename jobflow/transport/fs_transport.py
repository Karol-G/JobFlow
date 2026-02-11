from __future__ import annotations

import json
import logging
import os
import time
import uuid
from pathlib import Path

from .base import Transport
from ..messages import decode_message, encode_message

logger = logging.getLogger(__name__)


class FsTransport(Transport):
    def __init__(self, *, base_dir: Path, session_id: str, endpoint_id: str) -> None:
        self.endpoint_id = endpoint_id
        self.session_root = base_dir / session_id
        self._manager_inbox = self.session_root / "inbox" / "manager"
        self._workers_root = self.session_root / "inbox" / "workers"
        self._processed = self.session_root / "processed"

        self._manager_inbox.mkdir(parents=True, exist_ok=True)
        self._workers_root.mkdir(parents=True, exist_ok=True)
        self._processed.mkdir(parents=True, exist_ok=True)

        if endpoint_id == "manager":
            self._my_inbox = self._manager_inbox
        else:
            self._my_inbox = self._workers_root / endpoint_id
            self._my_inbox.mkdir(parents=True, exist_ok=True)

        self._pid = os.getpid()

    @property
    def mode(self) -> str:
        return "fs"

    def _target_inbox(self, dst_id: str) -> Path:
        if dst_id == "manager":
            return self._manager_inbox
        out = self._workers_root / dst_id
        out.mkdir(parents=True, exist_ok=True)
        return out

    def send(self, dst_id: str, msg: dict) -> None:
        inbox_dir = self._target_inbox(dst_id)
        msg_id = msg.get("msg_id") or str(uuid.uuid4())
        ts = int(time.time() * 1000)

        tmp_path = inbox_dir / f".tmp_{msg_id}_{uuid.uuid4().hex}.json"
        final_path = inbox_dir / f"msg_{ts}_{msg_id}.json"

        payload = encode_message(msg)
        with tmp_path.open("wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)

    def poll(self, timeout_s: float) -> list[dict]:
        deadline = time.time() + timeout_s
        messages: list[dict] = []

        while True:
            found = self._poll_once()
            if found:
                messages.extend(found)
                break

            if time.time() >= deadline:
                break
            time.sleep(0.05)

        return messages

    def _poll_once(self) -> list[dict]:
        out: list[dict] = []
        entries = sorted(self._my_inbox.glob("msg_*.json"))
        for path in entries:
            claim = path.with_suffix(path.suffix + f".claim.{self._pid}")
            try:
                os.replace(path, claim)
            except FileNotFoundError:
                continue
            except OSError:
                continue

            try:
                raw = claim.read_bytes()
                msg = decode_message(raw)
                out.append(msg)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in %s", claim)
            except Exception:
                logger.exception("Failed reading message file %s", claim)
            finally:
                try:
                    claim.unlink(missing_ok=True)
                except OSError:
                    logger.debug("Failed to remove claimed file %s", claim)
        return out

    def close(self) -> None:
        return
