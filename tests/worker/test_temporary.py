from celery import Celery
from celery.worker.worker import WorkController
from pytest_mock import MockerFixture

from dvc_task.worker.temporary import TemporaryWorker


def test_start(celery_app: Celery, mocker: MockerFixture):
    worker_cls = mocker.patch.object(celery_app, "Worker")
    thread = mocker.patch("threading.Thread")
    worker = TemporaryWorker(celery_app)
    worker.start()
    worker_cls.assert_called_once_with(app=celery_app, concurrency=None)
    thread.assert_called_once_with(target=worker.monitor, daemon=True)


def test_start_already_exists(
    celery_app: Celery,
    celery_worker: WorkController,
    mocker: MockerFixture,
):
    worker_cls = mocker.patch.object(celery_app, "Worker")
    thread = mocker.patch("threading.Thread")
    worker = TemporaryWorker(celery_app)
    worker.start()
    worker_cls.assert_not_called()
    thread.assert_not_called()


def test_monitor(
    celery_app: Celery,
    celery_worker: WorkController,
    mocker: MockerFixture,
):
    worker = TemporaryWorker(celery_app, timeout=1)
    shutdown = mocker.spy(celery_app.control, "shutdown")
    worker.monitor()
    shutdown.assert_called_once()
