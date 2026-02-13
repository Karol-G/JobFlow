from jobflow.manager import Manager


class _FakeStore:
    def task_counts(self):
        return {"QUEUED": 1}

    def worker_counts(self):
        return {"IDLE": 1}

    def launch_counts(self):
        return {"RUNNING": 1}


class _FakePublisher:
    def __init__(self) -> None:
        self.last_snapshot = None

    def publish(self, snapshot):
        self.last_snapshot = snapshot


class _FakeLogHandler:
    def snapshot(self):
        return ("line1", "line2")


def test_refresh_observers_includes_recent_logs() -> None:
    manager = Manager.__new__(Manager)
    manager.store = _FakeStore()
    manager._telemetry_log_handler = _FakeLogHandler()
    manager._telemetry_publisher_failed = False
    manager.telemetry_publisher = _FakePublisher()
    manager._last_telemetry_refresh_ts = 0.0
    manager.telemetry_refresh_s = 0.0
    manager._shutdown_started = False
    manager._pending_shutdown_acks = set()

    manager._refresh_observers(force=True)

    assert manager.telemetry_publisher.last_snapshot is not None
    assert manager.telemetry_publisher.last_snapshot.recent_logs == ("line1", "line2")
