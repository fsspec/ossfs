"""
Test all oss errors
"""
import inspect
import os
from typing import TYPE_CHECKING, Union

import pytest

# pylint:disable=missing-function-docstring


if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_bucket_name: str, test_directory: str):
    current_file_name = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"/{test_bucket_name}/{test_directory}/{current_file_name}"


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_errors(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"

    with pytest.raises(FileNotFoundError):
        ossfs.open(path + "none", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.mv(path + "x", path + "y")

    with pytest.raises(ValueError):
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
