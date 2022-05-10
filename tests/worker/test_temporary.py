"""Temporary Worker tests."""
import sys

import pytest
from celery import Celery
from celery.concurrency.prefork import TaskPool
from celery.worker.worker import WorkController
from pytest_mock import MockerFixture

from dvc_task.worker.temporary import TemporaryWorker


def test_start(celery_app: Celery, mocker: MockerFixture):
    """Should start underlying Celery worker."""
    worker_cls = mocker.patch.object(celery_app, "Worker")
    thread = mocker.patch("threading.Thread")
    worker = TemporaryWorker(
        celery_app, pool="prefork", concurrency=1, prefetch_multiplier=1
    )
    name = "worker1@localhost"
    worker.start(name)
    _args, kwargs = worker_cls.call_args
    assert kwargs["hostname"] == name
    assert kwargs["pool"] == TaskPool
    assert kwargs["concurrency"] == 1
    assert kwargs["prefetch_multiplier"] == 1
    thread.assert_called_once_with(
        target=worker.monitor, daemon=True, args=(name,)
    )


@pytest.mark.flaky(
    max_runs=3,
    rerun_filter=lambda *args: sys.platform == "darwin",
)
def test_start_already_exists(
    celery_app: Celery,
    celery_worker: WorkController,
    mocker: MockerFixture,
):
    """Should not start if worker instance already exists."""
    worker_cls = mocker.patch.object(celery_app, "Worker")
    thread = mocker.patch("threading.Thread")
    worker = TemporaryWorker(celery_app)
    worker.start(celery_worker.hostname)  # type: ignore[attr-defined]
    worker_cls.assert_not_called()
    thread.assert_not_called()


@pytest.mark.flaky(
    max_runs=3,
    rerun_filter=lambda *args: sys.platform == "darwin",
)
def test_monitor(
    celery_app: Celery,
    celery_worker: WorkController,
    mocker: MockerFixture,
):
    """Should shutdown worker when queue empty."""
    worker = TemporaryWorker(celery_app, timeout=1)
    shutdown = mocker.spy(celery_app.control, "shutdown")
    worker.monitor(celery_worker.hostname)  # type: ignore[attr-defined]
    shutdown.assert_called_once()
