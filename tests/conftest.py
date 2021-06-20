"""
Pytest setup
"""
# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name
import os
import subprocess
import time
import uuid

import pytest
import requests

from ossfs import OSSFileSystem

PORT = 5555
AccessKeyId = os.environ.get("OSS_ACCESS_KEY_ID", "")
AccessKeySecret = os.environ.get("OSS_SECRET_ACCESS_KEY", "")


test_id = uuid.uuid4()


@pytest.fixture()
def emulator_endpoint():
    return "http://127.0.0.1:%s/" % PORT


@pytest.fixture()
def endpoint():
    return os.environ.get("OSS_ENDPOINT")


@pytest.fixture()
def test_bucket_name():
    return os.environ.get("OSS_TEST_BUCKET_NAME")


@pytest.fixture()
def test_path(test_bucket_name):
    return f"/{test_bucket_name}/{test_id}"


@pytest.fixture()
def oss_emulator_server_start(emulator_endpoint):
    """
    Start a local emulator server
    """
    with subprocess.Popen("ruby bin/emulator -r store -p {}".format(PORT)):

        timeout = 5
        while timeout > 0:
            try:
                result = requests.get(emulator_endpoint)
                if result.ok:
                    break
            except Exception as err:
                raise Exception("emulator start timeout") from err
            timeout -= 0.1
            time.sleep(0.1)
        yield


@pytest.fixture()
def init_config(endpoint):
    result = {}
    result["key"] = AccessKeyId
    result["secret"] = AccessKeySecret
    result["endpoint"] = endpoint
    return result


@pytest.fixture()
def ossfs(init_config):
    return OSSFileSystem(**init_config)
