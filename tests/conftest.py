"""
Pytest setup
"""

import inspect
import os
import pathlib
import subprocess
import uuid
from typing import Dict, Optional, Union

import oss2
import pytest
import requests
from oss2 import Auth

from ossfs import AioOSSFileSystem, OSSFileSystem

PORT = 5555
AccessKeyId = os.environ.get("OSS_ACCESS_KEY_ID", "")
AccessKeySecret = os.environ.get("OSS_SECRET_ACCESS_KEY", "")
LICENSE_PATH = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), "LICENSE")
NUMBERS = b"1234567890\n"


test_id = uuid.uuid4()


def bucket_relative_path(path: str) -> str:
    return path.split("/", 2)[2]


def function_name(ossfs: Optional[Union["OSSFileSystem", "AioOSSFileSystem"]] = None):
    file_name = inspect.stack()[1].filename.rsplit(os.sep, maxsplit=1)[-1][:-3]
    function_name = inspect.stack()[1][0].f_code.co_name
    if ossfs:
        function_name += f"_{ossfs.__class__.__name__}"
    return f"{file_name}/{function_name}"


@pytest.fixture(scope="session")
def emulator_endpoint() -> str:
    return f"http://127.0.0.1:{PORT}/"


@pytest.fixture(scope="session")
def endpoint() -> str:
    endpoint = os.environ.get("OSS_ENDPOINT")
    assert endpoint
    return endpoint


@pytest.fixture(scope="session")
def test_bucket_name() -> str:
    test_bucket_name = os.environ.get("OSS_TEST_BUCKET_NAME")
    assert test_bucket_name
    return test_bucket_name


@pytest.fixture(scope="session", name="test_path")
def file_level_path(test_bucket_name: str, test_directory: str) -> str:
    return f"/{test_bucket_name}/{test_directory}"


@pytest.fixture(scope="session")
def anonymous_bucket_name() -> str:
    anonymous_bucket_name = os.environ.get("OSS_TEST_ANONYMOUS_BUCKET_NAME")
    assert anonymous_bucket_name
    return anonymous_bucket_name


@pytest.fixture(scope="session")
def test_directory() -> str:
    return f"ossfs_test/{test_id}"


@pytest.fixture
def oss_emulator_server_start(emulator_endpoint):
    """
    Start a local emulator server
    """
    with subprocess.Popen(f"ruby bin/emulator -r store -p {PORT}"):
        try:
            result = requests.get(emulator_endpoint, timeout=5)
            if result.ok:
                yield
        except TimeoutError as err:
            raise Exception("emulator start timeout") from err


@pytest.fixture(scope="session")
def init_config(endpoint) -> Dict:
    result = {}
    result["key"] = AccessKeyId
    result["secret"] = AccessKeySecret
    result["endpoint"] = endpoint
    return result


@pytest.fixture
def ossfs(request, init_config: Dict) -> Union["OSSFileSystem", "AioOSSFileSystem"]:
    if hasattr(request, "param") and request.param == "async":
        ossfs = AioOSSFileSystem(**init_config)
    else:
        ossfs = OSSFileSystem(**init_config)
    ossfs.invalidate_cache()
    return ossfs


@pytest.fixture(scope="session")
def auth():
    return Auth(AccessKeyId, AccessKeySecret)


@pytest.fixture(scope="session")
def bucket(auth: "Auth", endpoint: str, test_bucket_name: str):
    return oss2.Bucket(auth, endpoint, test_bucket_name)


@pytest.fixture(scope="session")
def file_in_anonymous(auth: "Auth", endpoint: str, anonymous_bucket_name: str) -> str:
    file = "file"
    bucket = oss2.Bucket(auth, endpoint, anonymous_bucket_name)
    bucket.put_object(file, "foobar")
    return f"/{anonymous_bucket_name}/{file}"


@pytest.fixture(scope="session")
def number_file(
    bucket: "oss2.Bucket", test_directory: str, test_bucket_name: str
) -> str:
    filename = f"{test_directory}/number"
    bucket.put_object(filename, NUMBERS)
    return f"/{test_bucket_name}/{filename}"


@pytest.fixture(scope="session")
def license_file(
    bucket: "oss2.Bucket", test_bucket_name: str, test_directory: str
) -> str:
    filename = f"{test_directory}/LICENSE"
    bucket.put_object_from_file(filename, LICENSE_PATH)
    return f"/{test_bucket_name}/{filename}"
