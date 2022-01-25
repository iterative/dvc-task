from typing import Dict, List, Optional, Union

from celery import shared_task

from .process import ManagedProcess


@shared_task(bind=True)
def run(
    self,
    args: Union[str, List[str]],
    env: Optional[Dict[str, str]] = None,
    wdir: Optional[str] = None,
    name: Optional[str] = None,
) -> Optional[int]:
    """Run a command inside a celery task.

    Arguments:
        args: Command to run.
        env: Optional environment variables.
        wdir: If specified, redirected output files will be placed in `wdir`.
        name: Name to use for this process, if not specified a UUID will be
            generated instead.
    """
    with ManagedProcess(args, env=env, wdir=wdir, name=name) as proc:
        self.update_state(state="RUNNING", meta=proc.info.asdict())
    return proc.info.asdict()
