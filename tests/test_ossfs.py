"""
Test class level functionality.
"""
import fsspec.core
import pytest

from ossfs import OSSFileSystem

TEST_BUCKET = "dvc-temp"
TEST_FILE_A = TEST_BUCKET + "/tmp/test/a"


@pytest.mark.parametrize("default_cache_type", ["none", "bytes", "readahead"])
def test_default_cache_type(init_config, default_cache_type):
    """
    Test set default cache type.
    """
    data = b"a" * (10 * 2 ** 20)
    init_config["default_cache_type"] = default_cache_type
    ossfs = OSSFileSystem(**init_config)
    with ossfs.open(TEST_FILE_A, "wb") as file:
        file.write(data)

    with ossfs.open(TEST_FILE_A, "rb") as file:
        assert isinstance(file.cache, fsspec.core.caches[default_cache_type])
        out = file.read(len(data))
        assert len(data) == len(out)
        assert out == data


@pytest.mark.parametrize("cache_type", ["none", "bytes", "readahead"])
def test_cache_type(ossfs, cache_type):
    """
    Test cache_type in open override default one.
    """
    data = b"a" * (10 * 2 ** 20)

    with ossfs.open(TEST_FILE_A, "wb") as file:
        file.write(data)

    with ossfs.open(TEST_FILE_A, "rb", cache_type=cache_type) as file:
        print(file.cache)
        assert isinstance(file.cache, fsspec.core.caches[cache_type])
        out = file.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_multiple_objects(init_config, ossfs):
    """
    Test multi OSSFS object
    """
    ossfs1 = OSSFileSystem(**init_config)
    ossfs2 = OSSFileSystem(**init_config)
    assert ossfs1.ls(TEST_BUCKET + "/test") == ossfs2.ls(TEST_BUCKET + "/test")
    assert ossfs.ls(TEST_BUCKET + "/test") == ossfs2.ls(TEST_BUCKET + "/test")
