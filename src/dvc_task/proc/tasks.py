"""Celery tasks."""
from typing import Any, Dict

from celery import shared_task

from .process import ManagedProcess


@shared_task(bind=True)
def run(  # pylint: disable=unused-argument
    self, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
    """Run a command inside a celery task.

    Accepts the same arguments as `proc.process.ManagedProcess`.
    """
    with ManagedProcess(*args, **kwargs) as proc:
        self.update_state(state="RUNNING", meta=proc.info.asdict())
    return proc.info.asdict()
