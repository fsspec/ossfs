"""
Test benchmark of get and push
"""

# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
import os
from typing import TYPE_CHECKING, Union

import pytest

from ..conftest import bucket_relative_path, function_name

if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_put_small_files(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    tmpdir,
    test_path: str,
    benchmark,
):
    path = f"{test_path}/{function_name(ossfs)}/"

    local_path = tmpdir / "number"
    local_path.mkdir()

    for num in range(1200):
        data = os.urandom(1000)
        local_file = local_path / str(num)
        with open(local_file, "wb") as f_w:
            f_w.write(data)

    benchmark.pedantic(
        ossfs.put,
        args=(str(local_path) + "/*", path),
        kwargs={},
        iterations=1,
        rounds=1,
    )
    assert len(ossfs.ls(path + "number/")) == 1200


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_get_small_files(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    tmpdir,
    bucket: "Bucket",
    test_path: str,
    benchmark,
):
    path = f"{test_path}/{function_name(ossfs)}/"

    for num in range(1200):
        data = os.urandom(1000)
        key = bucket_relative_path(path + str(num))
        bucket.put_object(key, data)

    local_path = tmpdir / "number"
    local_path.mkdir()
    local_dir = local_path / ossfs.__class__.__name__
    local_dir.mkdir()

    benchmark.pedantic(
        ossfs.get, args=(path + "*", str(local_path)), kwargs={}, iterations=1, rounds=1
    )
    assert len(local_dir.listdir()) == 1200
