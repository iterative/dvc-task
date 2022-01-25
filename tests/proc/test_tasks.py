from typing import Any, Dict

from celery import Celery
from celery.worker.worker import WorkController
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.proc.process import ManagedProcess
from dvc_task.proc.tasks import run


def test_run(
    tmp_dir: TmpDir,
    celery_app: Celery,
    celery_worker: WorkController,
    popen_pid: int,
    mocker: MockerFixture,
):
    env = {"FOO": "1"}
    wdir = str(tmp_dir / "wdir")
    name = "foo"
    init = mocker.spy(ManagedProcess, "__init__")
    result: Dict[str, Any] = run.delay(
        "/bin/foo", env=env, wdir=wdir, name=name
    ).get()
    assert result["pid"] == popen_pid
    init.assert_called_once_with(
        mocker.ANY, "/bin/foo", env=env, wdir=wdir, name=name
    )
