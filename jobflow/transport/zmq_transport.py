from __future__ import annotations

import logging
from typing import Optional

from .base import Transport
from ..messages import decode_message, encode_message

logger = logging.getLogger(__name__)


class ZmqTransport(Transport):
    def __init__(
        self,
        *,
        role: str,
        endpoint_id: str,
        bind_host: str = "0.0.0.0",
        manager_host: str = "127.0.0.1",
        port: int = 5555,
    ) -> None:
        try:
            import zmq
        except ImportError as exc:
            raise RuntimeError("pyzmq is required for --mode zmq") from exc

        self._zmq = zmq
        self.role = role
        self.endpoint_id = endpoint_id
        self._ctx = zmq.Context.instance()
        self._poller = zmq.Poller()

        if role == "manager":
            self._socket = self._ctx.socket(zmq.ROUTER)
            self._socket.bind(f"tcp://{bind_host}:{port}")
            logger.info("Manager ZMQ ROUTER bound at tcp://%s:%s", bind_host, port)
        elif role == "worker":
            self._socket = self._ctx.socket(zmq.DEALER)
            self._socket.setsockopt(zmq.IDENTITY, endpoint_id.encode("utf-8"))
            self._socket.connect(f"tcp://{manager_host}:{port}")
            logger.info("Worker ZMQ DEALER connected to tcp://%s:%s", manager_host, port)
        else:
            raise ValueError("role must be 'manager' or 'worker'")

        self._poller.register(self._socket, zmq.POLLIN)
        self._closed = False

    @property
    def mode(self) -> str:
        return "zmq"

    def send(self, dst_id: str, msg: dict) -> None:
        data = encode_message(msg)
        if self.role == "manager":
            self._socket.send_multipart([dst_id.encode("utf-8"), data])
        else:
            self._socket.send(data)

    def poll(self, timeout_s: float) -> list[dict]:
        timeout_ms = max(0, int(timeout_s * 1000))
        events = dict(self._poller.poll(timeout_ms))
        if self._socket not in events:
            return []

        out: list[dict] = []
        while True:
            try:
                if self.role == "manager":
                    identity, payload = self._socket.recv_multipart(flags=self._zmq.NOBLOCK)
                    msg = decode_message(payload)
                    src_id = msg.get("src", {}).get("id")
                    if src_id and src_id != identity.decode("utf-8", errors="replace"):
                        logger.debug("ZMQ identity mismatch: frame=%s src.id=%s", identity, src_id)
                    out.append(msg)
                else:
                    payload = self._socket.recv(flags=self._zmq.NOBLOCK)
                    out.append(decode_message(payload))
            except self._zmq.Again:
                break
        return out

    def close(self) -> None:
        if self._closed:
            return
        self._poller.unregister(self._socket)
        self._socket.close(linger=0)
        self._closed = True


def make_manager_transport(bind_host: str, port: int) -> ZmqTransport:
    return ZmqTransport(role="manager", endpoint_id="manager", bind_host=bind_host, port=port)


def make_worker_transport(worker_id: str, manager_host: str, port: int) -> ZmqTransport:
    return ZmqTransport(role="worker", endpoint_id=worker_id, manager_host=manager_host, port=port)
