"""
Test class level functionality.
"""

import pickle
import time
from multiprocessing.pool import ThreadPool
from typing import Dict, Union

import fsspec.core
import pytest

from ossfs import AioOSSFileSystem, OSSFileSystem
from ossfs.base import BaseOSSFileSystem
from tests.conftest import function_name


@pytest.mark.parametrize("aio", [False, True])
@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "readahead"])
def test_default_cache_type(
    init_config: Dict, default_cache_type: str, test_path: str, aio: bool
):
    path = f"{test_path}/{function_name()}/"
    data = b"a" * (10 * 2**20)
    file = path + "/test_default_cache_type/file"
    init_config["default_cache_type"] = default_cache_type
    if aio:  # noqa: SIM108
        ossfs = AioOSSFileSystem(**init_config)
    else:
        ossfs = OSSFileSystem(**init_config)
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(data)

    with ossfs.open(file, "rb") as f_rb:
        assert isinstance(f_rb.cache, fsspec.core.caches[default_cache_type])
        out = f_rb.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
@pytest.mark.parametrize("cache_type", ["none", "bytes", "readahead"])
def test_cache_type(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], cache_type: str, test_path: str
):
    path = f"{test_path}/{function_name(ossfs)}/"
    data = b"a" * (10 * 2**20)
    file = path + "/test_cache_type/file"

    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(data)

    with ossfs.open(file, "rb", cache_type=cache_type) as f_rb:
        assert isinstance(f_rb.cache, fsspec.core.caches[cache_type])
        out = f_rb.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_current(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], init_config: Dict):
    ossfs._cache.clear()
    ossfs = OSSFileSystem(**init_config)
    assert ossfs.current() is ossfs
    assert OSSFileSystem.current() is ossfs


def test_connect_many(init_config: Dict, test_bucket_name: str):
    def task(num):
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
    path = f"{test_path}/{function_name(ossfs)}/"
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
