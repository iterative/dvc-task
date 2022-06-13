"""(Local) filesystem based Celery application."""
import logging
import os
from typing import Any, Dict, Generator, Optional

from celery import Celery
from kombu.message import Message
from kombu.transport.filesystem import Channel
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads

from ..utils import makedirs, remove, unc_path

logger = logging.getLogger(__name__)

Channel.QoS.restore_at_shutdown = False


def _get_fs_config(
    wdir: str,
    mkdir: bool = False,
    task_serializer: str = "json",
    result_serializer: str = "json",
) -> Dict[str, Any]:
    broker_path = os.path.join(wdir, "broker")
    broker_control_path = unc_path(os.path.join(broker_path, "control"))
    broker_in_path = unc_path(os.path.join(broker_path, "in"))
    broker_processed_path = unc_path(os.path.join(broker_path, "processed"))
    result_path = os.path.join(wdir, "result")

    if mkdir:
        for path in (
            broker_control_path,
            broker_in_path,
            broker_processed_path,
            result_path,
        ):
            makedirs(path, exist_ok=True)

    return {
        "broker_url": "filesystem://",
        "broker_transport_options": {
            "control_folder": broker_control_path,
            "data_folder_in": broker_in_path,
            "data_folder_out": broker_in_path,
            "processed_folder": broker_processed_path,
            "store_processed": True,
        },
        "result_backend": f"file://{unc_path(result_path)}",
        "result_persistent": True,
        "task_serializer": task_serializer,
        "result_serializer": result_serializer,
        "accept_content": [task_serializer],
    }


class FSApp(Celery):
    """Local filesystem-based Celery application.

    Uses Kombu filesystem:// broker and results backend
    """

    def __init__(
        self,
        *args,
        wdir: Optional[str] = None,
        mkdir: bool = False,
        task_serializer: str = "json",
        result_serializer: str = "json",
        **kwargs: Any,
    ):
        """Construct an FSApp.

        Arguments:
            wdir: App broker/results directory. Defaults to current working
                directory.
            mkdir: Create broker/results subdirectories if they do not already
                exist.
            task_serializer: Default task serializer.
            result_serializer: Default result serializer.

        Additional arguments will be passed into the Celery constructor.
        """
        super().__init__(*args, **kwargs)
        self.wdir = wdir or os.getcwd()
        self.conf.update(
            _get_fs_config(
                self.wdir,
                mkdir=mkdir,
                task_serializer=task_serializer,
                result_serializer=result_serializer,
            )
        )
        logger.debug("Initialized filesystem:// app in '%s'", wdir)
        self._msg_path_cache: Dict[str, str] = {}

    def __reduce_keys__(self) -> Dict[str, Any]:
        keys = super().__reduce_keys__()  # type: ignore[misc]
        keys.update({"wdir": self.wdir})
        return keys

    def iter_queued(
        self, queue: Optional[str] = None
    ) -> Generator[Message, None, None]:
        """Iterate over queued tasks which have not been taken by a worker.

        Arguments:
            queue: Optional name of queue.
        """
        queue = queue or self.conf.task_default_queue
        with self.connection_for_read() as conn:  # type: ignore[attr-defined]
            with conn.channel() as channel:
                for filename in sorted(os.listdir(channel.data_folder_in)):
                    path = os.path.join(channel.data_folder_in, filename)
                    try:
                        with open(path, "rb") as fobj:
                            payload = fobj.read()
                    except FileNotFoundError:
                        # Messages returned by `listdir` call may have been
                        # acknowledged and moved to `processed_folder` by the
                        # time we try to read them here
                        continue
                    msg = channel.Message(
                        loads(bytes_to_str(payload)), channel=channel
                    )
                    self._msg_path_cache[msg.delivery_tag] = path
                    delivery_info = msg.properties.get("delivery_info", {})
                    if delivery_info.get("routing_key") == queue:
                        yield msg

    def reject(self, delivery_tag: str):
        """Reject the specified message.

        Allows the caller to reject FS broker messages without establishing a
        full Kombu consumer. Requeue is not supported.

        Raises:
            ValueError: Invalid delivery_tag
        """
        path = self._msg_path_cache.get(delivery_tag)
        if path and os.path.exists(path):
            remove(path)
            del self._msg_path_cache[delivery_tag]
            return

        for msg in self.iter_queued():
            if msg.delivery_tag == delivery_tag:
                remove(self._msg_path_cache[delivery_tag])
                del self._msg_path_cache[delivery_tag]
                return
        raise ValueError(f"Message '{delivery_tag}' not found")

    def iter_processed(
        self, queue: Optional[str] = None
    ) -> Generator[Message, None, None]:
        """Iterate over tasks which have been taken by a worker.

        Arguments:
            queue: Optional name of queue.
        """
        queue = queue or self.conf.task_default_queue
        with self.connection_for_read() as conn:  # type: ignore[attr-defined]
            with conn.channel() as channel:
                for filename in sorted(os.listdir(channel.processed_folder)):
                    with open(
                        os.path.join(channel.processed_folder, filename), "rb"
                    ) as fobj:
                        payload = fobj.read()
                    msg = channel.Message(
                        loads(bytes_to_str(payload)), channel=channel
                    )
                    delivery_info = msg.properties.get("delivery_info", {})
                    if delivery_info.get("routing_key") == queue:
                        yield msg
