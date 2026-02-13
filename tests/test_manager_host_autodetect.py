import argparse
from types import SimpleNamespace

from jobflow.manager import Manager, _detect_manager_host


def test_detect_manager_host_prefers_non_loopback(monkeypatch) -> None:
    monkeypatch.setattr(
        "jobflow.manager.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="127.0.0.1 10.11.12.13\n"),
    )
    assert _detect_manager_host("0.0.0.0") == "10.11.12.13"


def test_detect_manager_host_falls_back_to_bind_host(monkeypatch) -> None:
    monkeypatch.setattr("jobflow.manager.subprocess.run", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert _detect_manager_host("192.168.10.25") == "192.168.10.25"


def test_detect_manager_host_uses_socket_fallback(monkeypatch) -> None:
    class _FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def connect(self, _addr):
            return None

        def getsockname(self):
            return ("10.20.30.40", 9999)

    monkeypatch.setattr(
        "jobflow.manager.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=""),
    )
    monkeypatch.setattr("jobflow.manager.socket.socket", lambda *args, **kwargs: _FakeSocket())
    assert _detect_manager_host("0.0.0.0") == "10.20.30.40"


def test_normalize_args_autofills_manager_host_for_zmq(monkeypatch) -> None:
    monkeypatch.setattr(
        "jobflow.manager.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="10.0.2.44\n"),
    )
    args = argparse.Namespace(
        mode="zmq",
        manager_host=None,
        bind_host="0.0.0.0",
        telemetry_mode="off",
        telemetry_file=None,
        telemetry_queue_size=2,
        telemetry_refresh=0.5,
        db_path="/tmp/manager.db",
    )
    manager = Manager.__new__(Manager)
    manager._normalize_args(args)
    assert args.manager_host == "10.0.2.44"
