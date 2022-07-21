"""Temporary worker module."""
import logging
import os
import threading
import time
from typing import Any, List, Mapping

from celery import Celery
from celery.utils.nodenames import default_nodename

from dvc_task.app.filesystem import FSApp

logger = logging.getLogger(__name__)


class TemporaryWorker:
    """Temporary worker that automatically shuts down when queue is empty."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        app: Celery,
        timeout: int = 60,
        **kwargs,
    ):
        """Construct a worker.

        Arguments:
            app: Celery application instance.
            timeout: Queue timeout in seconds. Worker will be terminated if the
                queue remains empty after timeout.

        Additional keyword arguments will be passed as celery worker
        configuration.
        """
        self.app = app
        self.timeout = timeout
        self.config = kwargs

    def start(self, name: str) -> None:
        """Start the worker if it does not already exist.

        Runs the Celery worker main thread in the current process.

        Arguments:
            name: Celery worker name.
        """
        if os.name == "nt":
            # see https://github.com/celery/billiard/issues/247
            os.environ["FORKED_BY_MULTIPROCESSING"] = "1"

        if not self.app.control.ping(destination=[name]):
            monitor = threading.Thread(
                target=self.monitor, daemon=True, args=(name,)
            )
            monitor.start()
            config = dict(self.config)
            config["hostname"] = name
            argv = ["worker"]
            argv.extend(self._parse_config(config))
            self.app.worker_main(argv=argv)

    @staticmethod
    def _parse_config(config: Mapping[str, Any]) -> List[str]:
        loglevel = config.get("loglevel", "info")
        argv = [f"--loglevel={loglevel}"]
        for key in ("hostname", "pool", "concurrency", "prefetch_multiplier"):
            value = config.get(key)
            if value:
                argv_key = key.replace("_", "-")
                argv.append(f"--{argv_key}={value}")
        for key in (
            "without_heartbeat",
            "without_mingle",
            "without_gossip",
        ):
            if config.get(key):
                argv_key = key.replace("_", "-")
                argv.append(f"--{argv_key}")
        if config.get("task_events"):
            argv.append("-E")
        return argv

    def monitor(self, name: str) -> None:
        """Monitor the worker and stop it when the queue is empty."""
        logger.debug("monitor: waiting for worker to start")
        nodename = default_nodename(name)
        while not self.app.control.ping(destination=[nodename]):
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

            if isinstance(self.app, FSApp):
                yield from self.app.iter_queued()

        logger.info("monitor: watching celery worker '%s'", nodename)
        while self.app.control.ping(destination=[nodename]):
            time.sleep(self.timeout)
            nodes = self.app.control.inspect(  # type: ignore[call-arg]
                destination=[nodename]
            )
            if nodes is None or not any(tasks for tasks in _tasksets(nodes)):
                logger.info("monitor: shutting down due to empty queue.")
                self.app.control.shutdown(destination=[nodename])
                break
        logger.info("monitor: done")
