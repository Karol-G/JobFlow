from jobflow.telemetry import NoopTelemetryPublisher, TelemetrySnapshot


def test_noop_telemetry_publisher_accepts_snapshot() -> None:
    publisher = NoopTelemetryPublisher()
    snapshot = TelemetrySnapshot(
        ts=123.0,
        manager_pid=999,
        task_counts={"QUEUED": 5},
        worker_counts={"IDLE": 2},
        launch_counts={"RUNNING": 2},
        shutdown_started=False,
        pending_shutdown_acks=0,
    )

    publisher.publish(snapshot)
    publisher.close()
