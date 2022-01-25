import pytest
from pytest_mock import MockerFixture

TEST_PID = 1234


@pytest.fixture
def popen_pid(mocker: MockerFixture) -> int:
    mocker.patch(
        "subprocess.Popen",
        return_value=mocker.MagicMock(pid=TEST_PID, returncode=None),
    )
    return TEST_PID
