import json

from jobflow.telemetry import FileTelemetryPublisher, TelemetrySnapshot, snapshot_from_dict, snapshot_to_dict


def _snapshot(ts: float, queued: int) -> TelemetrySnapshot:
    return TelemetrySnapshot(
        ts=ts,
        manager_pid=1234,
        task_counts={"QUEUED": queued},
        worker_counts={"IDLE": 1},
        launch_counts={"RUNNING": 1},
        shutdown_started=False,
        pending_shutdown_acks=0,
    )


def test_snapshot_round_trip() -> None:
    original = _snapshot(3.0, 7)
    rebuilt = snapshot_from_dict(snapshot_to_dict(original))
    assert rebuilt.ts == original.ts
    assert rebuilt.manager_pid == original.manager_pid
    assert rebuilt.task_counts == original.task_counts
    assert rebuilt.worker_counts == original.worker_counts
    assert rebuilt.launch_counts == original.launch_counts


def test_file_telemetry_publisher_writes_latest_snapshot(tmp_path) -> None:
    out = tmp_path / "snapshot.json"
    publisher = FileTelemetryPublisher(out, max_queue=2)

    for i in range(50):
        publisher.publish(_snapshot(float(i), i))
    publisher.close()

    raw = json.loads(out.read_text(encoding="utf-8"))
    assert raw["ts"] == 49.0
    assert raw["task_counts"]["QUEUED"] == 49
