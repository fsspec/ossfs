"""tests for bucket level operations"""
# pylint:disable = too-many-arguments
# pylint:disable=missing-function-docstring
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

    auth = ossfs._auth  # pylint:disable=protected-access
    try:
        ossfs._auth = AnonymousAuth()  # pylint:disable=protected-access
        ossfs.invalidate_cache()
        bucket_infos = ossfs.ls("/")
        assert bucket_infos == []
        assert "cannot list buckets if not logged in" in caplog.text
    finally:
        ossfs._auth = auth  # pylint:disable=protected-access
