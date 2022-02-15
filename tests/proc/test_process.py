"""Process tests."""
import json
import subprocess
from typing import List, Union

import pytest
from pytest_mock import MockerFixture

from dvc_task.proc.exceptions import TimeoutExpired
from dvc_task.proc.process import ManagedProcess, ProcessInfo


@pytest.mark.usefixtures("tmp_dir", "popen_pid")
@pytest.mark.parametrize(
    "args",
    [
        "/bin/foo -o option",
        ["/bin/foo", "-o", "option"],
    ],
)
def test_init_args(args: Union[str, List[str]]):
    """Args should be shlex'd."""
    expected = ["/bin/foo", "-o", "option"]
    proc = ManagedProcess(args)
    assert expected == proc.args


@pytest.mark.usefixtures("tmp_dir")
def test_run(popen_pid: int):
    """Process info should be generated."""
    proc = ManagedProcess("/bin/foo")
    proc.run()
    assert popen_pid == proc.info.pid

    with open(proc.info_path, encoding="utf-8") as fobj:
        info = ProcessInfo.from_dict(json.load(fobj))
        assert popen_pid == info.pid


@pytest.mark.usefixtures("tmp_dir", "popen_pid")
def test_wait(mocker: MockerFixture):
    """Wait should block while process is running and incomplete."""
    with ManagedProcess("/bin/foo") as proc:
        # pylint: disable=protected-access
        proc._proc.wait = mocker.Mock(
            side_effect=subprocess.TimeoutExpired("/bin/foo", 5)
        )
        with pytest.raises(TimeoutExpired):
            proc.wait(5)

        proc._proc.wait = mocker.Mock(return_value=None)
        proc._proc.returncode = 0
        assert 0 == proc.wait()
        proc._proc.wait.assert_called_once()

        # once subprocess return code is set, future ManagedProcess.wait()
        # calls should not block
        proc._proc.wait.reset_mock()
        assert 0 == proc.wait()
        proc._proc.wait.assert_not_called()
