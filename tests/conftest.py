"""
Pytest setup
"""
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
import os
import pathlib
import subprocess
import uuid

import oss2
import pytest
import requests

from ossfs import OSSFileSystem

PORT = 5555
AccessKeyId = os.environ.get("OSS_ACCESS_KEY_ID", "")
AccessKeySecret = os.environ.get("OSS_SECRET_ACCESS_KEY", "")
LICENSE_PATH = os.path.join(
    pathlib.Path(__file__).parent.parent.resolve(), "LICENSE"
)
NUMBERS = b"1234567890\n"


test_id = uuid.uuid4()


@pytest.fixture(scope="session")
def emulator_endpoint():
    return f"http://127.0.0.1:{PORT}/"


@pytest.fixture(scope="session")
def endpoint():
    return os.environ.get("OSS_ENDPOINT")


@pytest.fixture(scope="session")
def test_bucket_name():
    return os.environ.get("OSS_TEST_BUCKET_NAME")


@pytest.fixture(scope="session")
def test_directory():
    return f"ossfs_test/{test_id}"


@pytest.fixture(scope="session")
def test_path(test_bucket_name, test_directory):
    return f"/{test_bucket_name}/{test_directory}"


@pytest.fixture()
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
def init_config(endpoint):
    result = {}
    result["key"] = AccessKeyId
    result["secret"] = AccessKeySecret
    result["endpoint"] = endpoint
    return result


@pytest.fixture()
def ossfs(init_config):
    return OSSFileSystem(**init_config)


def put_object(endpoint, bucket_name, filename, contents):
    auth = oss2.Auth(AccessKeyId, AccessKeySecret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    bucket.put_object(filename, contents)


def put_file(endpoint, bucket_name, key, filename):
    auth = oss2.Auth(AccessKeyId, AccessKeySecret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    bucket.put_object_from_file(key, filename)


@pytest.fixture(scope="session")
def file_in_anonymous(endpoint, test_directory):
    bucket = "dvc-anonymous"
    file = f"{test_directory}/file"
    put_object(endpoint, bucket, file, "foobar")
    return f"/{bucket}/{file}"


@pytest.fixture(scope="session")
def number_file(test_bucket_name, endpoint, test_directory):
    filename = f"{test_directory}/number"
    put_object(endpoint, test_bucket_name, filename, NUMBERS)
    return f"/{test_bucket_name}/{filename}"


@pytest.fixture(scope="session")
def license_file(test_bucket_name, endpoint, test_directory):
    filename = f"{test_directory}/LICENSE"
    put_file(endpoint, test_bucket_name, filename, LICENSE_PATH)
    return f"/{test_bucket_name}/{filename}"
