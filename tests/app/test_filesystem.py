"""Filesystem app tests."""
import json
import os
from typing import Optional

import pytest
from funcy import first
from kombu.message import Message
from pytest_test_utils import TmpDir

from dvc_task.app.filesystem import FSApp, _get_fs_config, _unc_path

TEST_MSG = {
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
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert config["broker_url"] == "filesystem://"


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_unc_path():
    """Windows paths should be converted to UNC paths."""
    assert "//?/c:/foo" == _unc_path(r"c:\foo")
    assert "//foo/bar" == _unc_path(r"\\foo\bar")


def test_fs_app(tmp_dir: TmpDir):
    """App should be constructed with filesystem broker/result config."""
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    assert app.wdir == str(tmp_dir)
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert app.conf["broker_url"] == "filesystem://"


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
