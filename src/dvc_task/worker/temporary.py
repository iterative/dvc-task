"""Temporary worker module."""
import logging
import threading
import time
from typing import Optional

from celery import Celery

logger = logging.getLogger(__name__)


class TemporaryWorker:
    """Temporary worker that automatically shuts down when queue is empty."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        app: Celery,
        timeout: int = 60,
        concurrency: Optional[int] = None,
        loglevel: Optional[str] = None,
        task_events: bool = True,
    ):
        """Construct a worker.

        Arguments:
            app: Celery application instance.
            timeout: Queue timeout in seconds. Worker will be terminated if the
            queue remains empty after timeout.
            concurrency: Worker concurrency.
            loglevel: Worker loglevel.
            task_events: Enable worker task event monitoring.
        """
        self.app = app
        self.timeout = timeout
        self.concurrency = concurrency
        self.loglevel = loglevel or "info"
        self.task_events = task_events

    def start(self, name: str) -> None:
        """Start the worker if it does not already exist.

        Runs the Celery worker main thread in the current process.

        Arguments:
            name: Celery worker name.
        """
        if not self.app.control.ping(destination=[name]):
            monitor = threading.Thread(
                target=self.monitor, daemon=True, args=(name,)
            )
            monitor.start()
            argv = [
                "worker",
                f"--loglevel={self.loglevel}",
                f"--hostname={name}",
            ]
            if self.concurrency:
                argv.append(f"--concurrency={self.concurrency}")
            if self.task_events:
                argv.append("-E")
            self.app.worker_main(argv=argv)

    def monitor(self, name: str) -> None:
        """Monitor the worker and stop it when the queue is empty."""
        logger.debug("monitor: waiting for worker to start")
        while not self.app.control.ping(destination=[name]):
            # wait for worker to start
            time.sleep(1)

        def _tasksets(nodes):
            for taskset in (
                nodes.active(),
                nodes.scheduled(),
                nodes.reserved(),
            ):
                if taskset is not None:
                    yield from taskset.values()

        logger.info("monitor: watching celery worker '%s'", name)
        while self.app.control.ping(destination=[name]):
            time.sleep(self.timeout)
            nodes = self.app.control.inspect(  # type: ignore[call-arg]
                destination=[name]
            )
            if nodes is None or not any(tasks for tasks in _tasksets(nodes)):
                logger.info("monitor: shutting down due to empty queue.")
                self.app.control.shutdown(destination=[name])
                break
        logger.info("monitor: done")
