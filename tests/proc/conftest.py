"""Process test fixtures."""
import json
import os
from typing import Optional

import pytest
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.proc.manager import ProcessManager
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


@pytest.fixture(name="process_manager")
def fixture_process_manager(tmp_dir: TmpDir) -> ProcessManager:
    """Return a process manager which uses tmp_dir as the working dir."""
    return ProcessManager(tmp_dir)


def create_process(
    manager: ProcessManager,
    name: str,
    pid: int,
    returncode: Optional[int] = None,
):
    """Create a test process info directory."""
    info_path = manager._get_info_path(  # pylint: disable=protected-access
        name
    )
    os.makedirs(os.path.dirname(info_path))
    process_info = ProcessInfo(
        pid=pid,
        stdin=None,
        stdout=f"{name}.out",
        stderr=None,
        returncode=returncode,
    )
    with open(info_path, "w", encoding="utf-8") as fobj:
        json.dump(process_info.asdict(), fobj)


@pytest.fixture
def finished_process(process_manager: ProcessManager) -> str:
    """Return a finished test process."""
    key = "finished"
    create_process(process_manager, key, PID_FINISHED, 0)
    return key


@pytest.fixture
def running_process(process_manager: ProcessManager) -> str:
    """Return a running test process."""
    key = "running"
    create_process(process_manager, key, PID_RUNNING)
    return key
