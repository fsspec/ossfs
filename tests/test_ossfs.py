"""
Test class level functionality.
"""
# pylint:disable=protected-access
# pylint:disable=missing-function-docstring
# pylint:disable=invalid-name
import pickle
import time
from multiprocessing.pool import ThreadPool

import fsspec.core
import pytest

from ossfs import OSSFileSystem


@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "readahead"])
def test_default_cache_type(init_config, default_cache_type, test_path):
    data = b"a" * (10 * 2 ** 20)
    file = test_path + "/test_default_cache_type/file"
    init_config["default_cache_type"] = default_cache_type
    ossfs = OSSFileSystem(**init_config)
    with ossfs.open(file, "wb") as f_w:
        f_w.write(data)

    with ossfs.open(file, "rb") as f_r:
        assert isinstance(f_r.cache, fsspec.core.caches[default_cache_type])
        out = f_r.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("cache_type", ["none", "bytes", "readahead"])
def test_cache_type(ossfs, cache_type, test_path):
    data = b"a" * (10 * 2 ** 20)
    file = test_path + "/test_cache_type/file"

    with ossfs.open(file, "wb") as f:
        f.write(data)

    with ossfs.open(file, "rb", cache_type=cache_type) as f:
        print(f.cache)
        assert isinstance(f.cache, fsspec.core.caches[cache_type])
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_current(ossfs, init_config):
    ossfs._cache.clear()  # pylint: disable=protected-access
    ossfs = OSSFileSystem(**init_config)
    assert ossfs.current() is ossfs
    assert OSSFileSystem.current() is ossfs


def test_connect_many(init_config, test_bucket_name):
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


def test_pickle(ossfs, test_path):

    path = test_path + "/test_pickle/"
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
    assert OSSFileSystem._strip_protocol(address) == "/mybucket/myobject"
    address = "oss://mybucket/myobject"
    assert OSSFileSystem._strip_protocol(address) == "/mybucket/myobject"
