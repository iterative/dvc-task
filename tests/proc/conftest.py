"""Process test fixtures."""
import json
import os
from typing import Optional

import pytest
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.proc.process import ProcessInfo

TEST_PID = 1234
PID_FINISHED = 1234
PID_RUNNING = 5678


@pytest.fixture
def popen_pid(mocker: MockerFixture) -> int:
    """Return a mocked Popen PID."""
    mocker.patch(
        "subprocess.Popen",
        return_value=mocker.MagicMock(pid=TEST_PID, returncode=None),
    )
    return TEST_PID


def create_process(
    root: str, name: str, pid: int, returncode: Optional[int] = None
):
    """Create a test process info directory."""
    info_path = os.path.join(root, name, f"{name}.json")
    os.makedirs(os.path.join(root, name))
    process_info = ProcessInfo(
        pid=pid, stdin=None, stdout=None, stderr=None, returncode=returncode
    )
    with open(info_path, "w", encoding="utf-8") as fobj:
        json.dump(process_info.asdict(), fobj)


@pytest.fixture
def finished_process(tmp_dir: TmpDir) -> str:
    """Return a finished test process."""
    key = "finished"
    create_process(tmp_dir, key, PID_FINISHED, 0)
    return key


@pytest.fixture
def running_process(tmp_dir: TmpDir) -> str:
    """Return a running test process."""
    key = "running"
    create_process(tmp_dir, key, PID_RUNNING)
    return key
