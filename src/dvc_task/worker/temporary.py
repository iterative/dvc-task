import logging
import threading
import time
from typing import Optional

from celery import Celery
from funcy import concat

logger = logging.getLogger(__name__)


class TemporaryWorker:
    """Temporary worker that automatically shuts down when queue is empty."""

    def __init__(
        self,
        app: Celery,
        timeout: int = 60,
        concurrency: Optional[int] = None,
    ):
        self.app = app
        self.timeout = timeout
        self.concurrency = concurrency

    def start(self) -> None:
        """Start the worker if it does not already exist.

        Note:
            This runs the Celery worker in the current process.
        """
        if not self.app.control.ping():
            worker = self.app.Worker(
                app=self.app, concurrency=self.concurrency
            )
            worker.start()
            monitor = threading.Thread(target=self.monitor, daemon=True)
            monitor.start()

    def monitor(self) -> None:
        """Monitor the worker and stop it when the queue is empty."""
        while self.app.control.ping():
            time.sleep(self.timeout)
            nodes = self.app.control.inspect()
            if not any(
                tasks
                for tasks in concat(
                    nodes.active().values(),
                    nodes.scheduled().values(),
                    nodes.reserved().values(),
                )
            ):
                logger.debug("Shutting down workers due to empty queue.")
                self.app.control.shutdown()
                break
