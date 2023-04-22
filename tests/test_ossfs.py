"""
Test class level functionality.
"""
import inspect

# pylint:disable=protected-access
# pylint:disable=missing-function-docstring
# pylint:disable=invalid-name
import os
import pickle
import time
from multiprocessing.pool import ThreadPool
from typing import Dict, Union

import fsspec.core
import pytest

from ossfs import AioOSSFileSystem, OSSFileSystem
from ossfs.base import BaseOSSFileSystem


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_bucket_name: str, test_directory: str):
    file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"/{test_bucket_name}/{test_directory}/{file_name}"


@pytest.mark.parametrize("aio", [False, True])
@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "readahead"])
def test_default_cache_type(
    init_config: Dict, default_cache_type: str, test_path: str, aio: bool
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    data = b"a" * (10 * 2**20)
    file = path + "/test_default_cache_type/file"
    init_config["default_cache_type"] = default_cache_type
    if aio:
        ossfs = AioOSSFileSystem(**init_config)
    else:
        ossfs = OSSFileSystem(**init_config)
    with ossfs.open(file, "wb") as f:
        f.write(data)

    with ossfs.open(file, "rb") as f:
        assert isinstance(f.cache, fsspec.core.caches[default_cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
@pytest.mark.parametrize("cache_type", ["none", "bytes", "readahead"])
def test_cache_type(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], cache_type: str, test_path: str
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    data = b"a" * (10 * 2**20)
    file = path + "/test_cache_type/file"

    with ossfs.open(file, "wb") as f:
        f.write(data)

    with ossfs.open(file, "rb", cache_type=cache_type) as f:
        assert isinstance(f.cache, fsspec.core.caches[cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_current(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], init_config: Dict):
    ossfs._cache.clear()  # pylint: disable=protected-access
    ossfs = OSSFileSystem(**init_config)
    assert ossfs.current() is ossfs
    assert OSSFileSystem.current() is ossfs


def test_connect_many(init_config: Dict, test_bucket_name: str):
    def task(num):  # pylint: disable=unused-argument
        ossfs = OSSFileSystem(**init_config)
        ossfs.ls(test_bucket_name)
        time.sleep(5)
        ossfs.ls(test_bucket_name)
        return True

    pool = ThreadPool(processes=20)
    out = pool.map(task, range(40))
    assert all(out)
    pool.close()
    pool.join()


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_pickle(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    for number in range(10):
        ossfs.touch(path + "file" + str(number))

    ossfs1 = pickle.loads(pickle.dumps(ossfs))
    assert ossfs.ls(path) == ossfs1.ls(path)
    ossfs2 = pickle.loads(pickle.dumps(ossfs1))
    assert ossfs.ls(path) == ossfs2.ls(path)


def test_strip_protocol():
    """
    Test protocols
    """
    address = "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
    assert BaseOSSFileSystem._strip_protocol(address) == "/mybucket/myobject"
    address = "oss://mybucket/myobject"
    assert BaseOSSFileSystem._strip_protocol(address) == "/mybucket/myobject"
