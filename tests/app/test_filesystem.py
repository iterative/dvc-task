import os

import pytest
from pytest_mock import MockerFixture
from pytest_test_utils import TmpDir

from dvc_task.app.filesystem import FSApp, _get_fs_config, _unc_path


def test_config(tmp_dir: TmpDir):
    config = _get_fs_config(str(tmp_dir), mkdir=True)
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert config["broker_url"] == "filesystem://"


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_unc_path():
    assert "//?/c:/foo" == _unc_path(r"c:\foo")
    assert "//foo/bar" == _unc_path(r"\\foo\bar")


def test_fs_app(tmp_dir: TmpDir, mocker: MockerFixture):
    app = FSApp(wdir=str(tmp_dir), mkdir=True)
    assert app.wdir == str(tmp_dir)
    assert (tmp_dir / "broker" / "in").is_dir()
    assert (tmp_dir / "broker" / "processed").is_dir()
    assert (tmp_dir / "result").is_dir()
    assert app.conf["broker_url"] == "filesystem://"
