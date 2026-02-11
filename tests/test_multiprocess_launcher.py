import sys
import time

from jobflow.launcher.multiprocess import MultiprocessLauncher


def test_multiprocess_launcher_submit_poll_cancel():
    launcher = MultiprocessLauncher()
    command = [sys.executable, "-c", "import time; time.sleep(30)"]
    records = launcher.submit_workers(1, command, env={}, requested={})

    assert len(records) == 1
    record = records[0]
    assert record.system == "multiprocess"
    assert record.batch_job_id is not None

    status = launcher.poll([record.batch_job_id])[record.batch_job_id]
    assert status["state"] == "RUNNING"

    launcher.cancel(record.batch_job_id)

    for _ in range(40):
        status = launcher.poll([record.batch_job_id])[record.batch_job_id]
        if status["state"] == "FAILED":
            break
        time.sleep(0.05)

    assert status["state"] == "FAILED"
