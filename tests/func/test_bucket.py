"""tests for bucket level operations"""

from typing import TYPE_CHECKING, Union

import pytest
from oss2.auth import AnonymousAuth

if TYPE_CHECKING:
    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ls_bucket(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_bucket_name: str,
    anonymous_bucket_name: str,
    caplog,
):
    bucket_infos = ossfs.ls("/")
    bucket_info_dict = {}
    for bucket_info in bucket_infos:
        bucket_info_dict[bucket_info["name"]] = bucket_info
    assert f"/{test_bucket_name}" in bucket_info_dict
    assert f"/{anonymous_bucket_name}" in bucket_info_dict

    auth = ossfs._auth
    try:
        ossfs._auth = AnonymousAuth()
        ossfs.invalidate_cache()
        bucket_infos = ossfs.ls("/")
        assert bucket_infos == []
        assert "cannot list buckets if not logged in" in caplog.text
    finally:
        ossfs._auth = auth


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_bucket_is_dir(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_bucket_name: str,
    anonymous_bucket_name: str,
):
    assert ossfs.isdir("")
    assert ossfs.isdir("/")
    assert ossfs.isdir(test_bucket_name)
    assert not ossfs.isdir("non-exist-bucket")
    assert ossfs.isdir(f"/{anonymous_bucket_name}")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_bucket_is_file(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_bucket_name: str,
    anonymous_bucket_name: str,
):
    assert not ossfs.isfile("")
    assert not ossfs.isfile("/")
    assert not ossfs.isfile(test_bucket_name)
    assert not ossfs.isfile("non-exist-bucket")
    assert not ossfs.isfile(f"/{anonymous_bucket_name}")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_bucket_exists(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_bucket_name: str,
    anonymous_bucket_name: str,
):
    assert ossfs.exists("")
    assert ossfs.exists("/")
    assert ossfs.exists(test_bucket_name)
    assert not ossfs.exists("non-exist-bucket")
    assert ossfs.exists(f"/{anonymous_bucket_name}")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_bucket_info(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_bucket_name: str,
    anonymous_bucket_name: str,
):
    assert ossfs.info("") == {
        "Key": "",
        "Size": 0,
        "type": "directory",
        "size": 0,
        "name": "",
    }
    assert ossfs.info("/") == {
        "Key": "/",
        "Size": 0,
        "type": "directory",
        "size": 0,
        "name": "/",
    }
    assert ossfs.info(test_bucket_name) == {
        "Key": test_bucket_name,
        "type": "directory",
        "Size": 0,
        "size": 0,
        "name": test_bucket_name,
    }
    with pytest.raises(FileNotFoundError):
        assert ossfs.info("non-exist-bucket")
    assert ossfs.info(f"/{anonymous_bucket_name}") == {
        "Key": f"/{anonymous_bucket_name}",
        "type": "directory",
        "Size": 0,
        "size": 0,
        "name": f"/{anonymous_bucket_name}",
    }
