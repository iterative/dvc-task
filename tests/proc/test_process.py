import json
import subprocess

import pytest
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.proc.process import ManagedProcess, ProcessInfo

TEST_PID = 1234


@pytest.fixture(autouse=True)
def mock_popen(mocker: MockerFixture):
    mocker.patch(
        "subprocess.Popen",
        return_value=mocker.MagicMock(pid=TEST_PID, returncode=None),
    )


@pytest.mark.parametrize(
    "args",
    [
        "/bin/foo -o option",
        ["/bin/foo", "-o", "option"],
    ],
)
def test_init_args(tmp_dir: TmpDir, args, mocker: MockerFixture):
    expected = ["/bin/foo", "-o", "option"]
    proc = ManagedProcess(args)
    assert expected == proc.args


def test_run(tmp_dir: TmpDir, mocker: MockerFixture):
    proc = ManagedProcess("/bin/foo")
    assert TEST_PID == proc.pid

    with open(proc.info_path, encoding="utf-8") as fobj:
        info = ProcessInfo.from_dict(json.load(fobj))
        assert TEST_PID == info.pid


def test_wait(tmp_dir: TmpDir, mocker: MockerFixture):
    from dvc_task.proc.exceptions import TimeoutExpired

    proc = ManagedProcess("/bin/foo")
    proc._proc.wait = mocker.Mock(
        side_effect=subprocess.TimeoutExpired("/bin/foo", 5)
    )
    with pytest.raises(TimeoutExpired):
        proc.wait(5)

    proc._proc.wait = mocker.Mock(return_value=None)
    proc._proc.returncode = 0
    assert 0 == proc.wait()
    proc._proc.wait.assert_called_once()

    # once subprocess return code is set, future ManagedProcess.wait() calls
    # should not block
    proc._proc.wait.reset_mock()
    assert 0 == proc.wait()
    proc._proc.wait.assert_not_called()
