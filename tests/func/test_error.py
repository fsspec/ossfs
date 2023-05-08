"""
Test all oss errors
"""
# pylint:disable=missing-function-docstring
from typing import TYPE_CHECKING, Union

import pytest

from ..conftest import function_name

if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_errors(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    path = f"{test_path}/{function_name(ossfs)}/"

    with pytest.raises(FileNotFoundError):
        ossfs.open(path + "none", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.mv(path + "x", path + "y")

    with pytest.raises(FileNotFoundError):
        ossfs.open("x", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.open("xxxx", "rb")

    with pytest.raises(PermissionError):
        ossfs.rm("/non-exist-bucket")

    with pytest.raises(ValueError):
        with ossfs.open(path + "temp", "wb") as file_obj:
            file_obj.read()

    bucket.put_object(path.split("/", 2)[-1] + "temp", "foobar")
    with pytest.raises(ValueError):
        file_obj = ossfs.open(path + "temp", "rb")  # pylint:disable=consider-using-with
        file_obj.close()
        file_obj.read()
