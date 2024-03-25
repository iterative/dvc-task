"""Utils tests."""

import os

import pytest

from dvc_task.utils import unc_path


@pytest.mark.skipif(os.name != "nt", reason="Windows only")
def test_unc_path():
    """Windows paths should be converted to UNC paths."""
    assert unc_path(r"c:\foo") == r"\\?\c:\foo"
    assert unc_path(r"\\foo\bar") == r"\\foo\bar"
