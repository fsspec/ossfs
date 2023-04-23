"""
Test benchmark of get and push
"""
import inspect

# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
# pylint:disable=consider-using-with
import os
from typing import TYPE_CHECKING, Union

import pytest

from ..conftest import bucket_relative_path

if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.fixture(name="test_path", scope="module")
def file_name_path(test_bucket_name: str, test_directory: str):
    """Add current file name to the path name"""
    file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"/{test_bucket_name}/{test_directory}/{file_name}"


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_put_small_files(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    tmpdir,
    test_path: str,
    benchmark,
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"

    local_path = tmpdir / "number"
    local_path.mkdir()

    for num in range(1200):
        data = os.urandom(1000)
        local_file = local_path / str(num)
        open(local_file, "wb").write(data)

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
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"

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
