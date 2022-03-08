"""Process manager tests."""
import builtins
import signal
import sys

import pytest
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.proc.exceptions import (
    ProcessNotTerminatedError,
    UnsupportedSignalError,
)
from dvc_task.proc.manager import ProcessManager
from dvc_task.proc.process import ProcessInfo

from .conftest import PID_RUNNING


def test_send_signal(
    mocker: MockerFixture,
    process_manager: ProcessManager,
    finished_process: str,
    running_process: str,
):
    """Terminate signal should be sent."""
    mock_kill = mocker.patch("os.kill")
    process_manager.send_signal(running_process, signal.SIGTERM)
    mock_kill.assert_called_once_with(PID_RUNNING, signal.SIGTERM)

    mock_kill.reset_mock()
    process_manager.send_signal(finished_process, signal.SIGTERM)
    mock_kill.assert_not_called()

    if sys.platform == "win32":
        with pytest.raises(UnsupportedSignalError):
            process_manager.send_signal(finished_process, signal.SIGABRT)


def test_dead_process(
    mocker: MockerFixture,
    process_manager: ProcessManager,
    running_process: str,
):
    """Dead process lookup should fail."""

    def side_effect(*args):
        if sys.platform == "win32":
            err = OSError()
            err.winerror = 87
            raise err
        raise ProcessLookupError()

    mocker.patch("os.kill", side_effect=side_effect)
    with pytest.raises(ProcessLookupError):
        process_manager.send_signal(running_process, signal.SIGTERM)
    assert process_manager[running_process].returncode == -1


def test_kill(
    mocker: MockerFixture,
    process_manager: ProcessManager,
    finished_process: str,
    running_process: str,
):
    """Kill signal should be sent."""
    mock_kill = mocker.patch("os.kill")
    process_manager.kill(running_process)
    if sys.platform == "win32":
        mock_kill.assert_called_once_with(PID_RUNNING, signal.SIGTERM)
    else:
        mock_kill.assert_called_once_with(
            PID_RUNNING, signal.SIGKILL  # pylint: disable=no-member
        )

    mock_kill.reset_mock()
    process_manager.kill(finished_process)
    mock_kill.assert_not_called()


def test_terminate(
    mocker: MockerFixture,
    process_manager: ProcessManager,
    running_process: str,
    finished_process: str,
):
    """Terminate signal should be sent."""
    mock_kill = mocker.patch("os.kill")
    process_manager.terminate(running_process)
    mock_kill.assert_called_once_with(PID_RUNNING, signal.SIGTERM)

    mock_kill.reset_mock()
    process_manager.terminate(finished_process)
    mock_kill.assert_not_called()


def test_remove(
    mocker: MockerFixture,
    tmp_dir: TmpDir,
    process_manager: ProcessManager,
    running_process: str,
    finished_process: str,
):
    """Process should be removed."""
    mocker.patch("os.kill", return_value=None)
    process_manager.remove(finished_process)
    assert not (tmp_dir / finished_process).exists()
    with pytest.raises(ProcessNotTerminatedError):
        process_manager.remove(running_process)
    assert (tmp_dir / running_process).exists()
    process_manager.remove(running_process, True)
    assert not (tmp_dir / running_process).exists()


@pytest.mark.parametrize("force", [True, False])
def test_cleanup(  # pylint: disable=too-many-arguments
    mocker: MockerFixture,
    tmp_dir: TmpDir,
    process_manager: ProcessManager,
    running_process: str,
    finished_process: str,
    force: bool,
):
    """Process directory should be removed."""
    mocker.patch("os.kill", return_value=None)
    process_manager.cleanup(force)
    assert (tmp_dir / running_process).exists() != force
    assert not (tmp_dir / finished_process).exists()


def test_follow(
    mocker: MockerFixture,
    process_manager: ProcessManager,
    running_process: str,
):
    """Output should be followed and not duplicated."""
    orig_open = builtins.open
    mock_file = mocker.mock_open()()
    expected = ["foo\n", "bar\n", "b", "", "az\n"]
    mock_file.readline = mocker.Mock(side_effect=expected)

    def _open(path, *args, **kwargs):
        if path.endswith(".out"):
            return mock_file
        return orig_open(path, *args, **kwargs)

    mocker.patch("builtins.open", _open)
    mock_sleep = mocker.patch("time.sleep")
    follow_gen = process_manager.follow(running_process)
    for line in expected:
        if line:
            assert line == next(follow_gen)
    mock_sleep.assert_called_once_with(1)

    # Process exit with no further output should cause StopIteration
    # (raised as RuntimeError)
    mocker.patch.object(
        process_manager,
        "__getitem__",
        return_value=ProcessInfo(
            pid=PID_RUNNING, stdin=None, stdout=None, stderr=None, returncode=0
        ),
    )
    with pytest.raises(RuntimeError):
        next(follow_gen)
