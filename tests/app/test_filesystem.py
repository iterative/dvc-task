"""Filesystem app tests."""
import json
from typing import Any, Dict, Optional

import pytest
from celery.backends.filesystem import FilesystemBackend
from funcy import first
from kombu.message import Message
from pytest_test_utils import TmpDir

from dvc_task.app.filesystem import FSApp, _get_fs_config

TEST_MSG: Dict[str, Any] = {
    "body": "",
    "content-encoding": "utf-8",
    "content-type": "application/json",
    "headers": {},
    "properties": {
        "correlation_id": "123",
        "reply_to": "456",
        "delivery_mode": 2,
        "delivery_info": {"exchange": "", "routing_key": "celery"},
        "priority": 0,
        "body_encoding": "base64",
        "delivery_tag": "789",
    },
}


def test_config(tmp_dir: TmpDir):
    """Should return a filesystem broker/resut config."""
    config = _get_fs_config(str(tmp_dir), mkdir=True)
    assert (tmp_dir / "broker" / "control").is_dir()
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert config["broker_url"] == "filesystem://"


def test_fs_app(tmp_dir: TmpDir):
    """App should be constructed with filesystem broker/result config."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    assert app.wdir == str(tmp_dir)
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert app.conf["broker_url"] == "filesystem://"
    backend = app.backend
    assert isinstance(backend, FilesystemBackend)
    assert backend.url == app.conf.result_backend


def test_iter_queued(tmp_dir: TmpDir):
    """App should iterate over messages in 'broker/in'."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    msg: Optional[Message] = first(app.iter_queued())
    assert msg is None

    tmp_dir.gen({"broker": {"in": {"foo.msg": json.dumps(TEST_MSG)}}})
    msg = first(app.iter_queued())
    assert msg is not None
    for key, value in TEST_MSG.items():
        attr = getattr(msg, key.replace("-", "_"))
        if isinstance(attr, bytes):
            attr = attr.decode("utf-8")
        assert attr == value
    assert first(app.iter_processed()) is None


def test_iter_processed(tmp_dir: TmpDir):
    """App should iterate over messages in 'broker/processed'."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    msg: Optional[Message] = first(app.iter_processed())
    assert msg is None

    tmp_dir.gen({"broker": {"processed": {"foo.msg": json.dumps(TEST_MSG)}}})
    msg = first(app.iter_processed())
    assert msg is not None
    for key, value in TEST_MSG.items():
        attr = getattr(msg, key.replace("-", "_"))
        if isinstance(attr, bytes):
            attr = attr.decode("utf-8")
        assert attr == value
    assert first(app.iter_queued()) is None


def test_reject(tmp_dir: TmpDir):
    """Rejected message should be removed."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    tmp_dir.gen({"broker": {"in": {"foo.msg": json.dumps(TEST_MSG)}}})

    app.reject(TEST_MSG["properties"]["delivery_tag"])
    assert not (tmp_dir / "broker" / "in" / "foo.msg").exists()

    tmp_dir.gen({"broker": {"in": {"foo.msg": json.dumps(TEST_MSG)}}})
    for msg in app.iter_queued():
        assert msg.delivery_tag
        app.reject(msg.delivery_tag)
    assert not (tmp_dir / "broker" / "in" / "foo.msg").exists()

    with pytest.raises(ValueError):
        app.reject(TEST_MSG["properties"]["delivery_tag"])


def test_purge(tmp_dir: TmpDir):
    """Purge message should be removed."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    tmp_dir.gen({"broker": {"processed": {"foo.msg": json.dumps(TEST_MSG)}}})

    app.purge(TEST_MSG["properties"]["delivery_tag"])
    assert not (tmp_dir / "broker" / "processed" / "foo.msg").exists()

    tmp_dir.gen({"broker": {"processed": {"foo.msg": json.dumps(TEST_MSG)}}})
    for msg in app.iter_processed():
        assert msg.delivery_tag
        app.purge(msg.delivery_tag)
    assert not (tmp_dir / "broker" / "processed" / "foo.msg").exists()

    with pytest.raises(ValueError):
        app.purge(TEST_MSG["properties"]["delivery_tag"])
