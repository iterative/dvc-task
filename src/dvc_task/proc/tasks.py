"""Celery tasks."""

from typing import Any

from celery import shared_task

from .process import ManagedProcess


@shared_task(bind=True)
def run(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """Run a command inside a celery task.

    Accepts the same arguments as `proc.process.ManagedProcess`.
    """
    with ManagedProcess(*args, **kwargs) as proc:
        pass
    return proc.info.asdict()
