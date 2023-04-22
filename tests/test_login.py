"""
Three different login test
Sts, anonymous, accesskey
"""
# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
import json
import os
from typing import Union

import oss2
import pytest
from aliyunsdkcore import client
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

from ossfs import AioOSSFileSystem, OSSFileSystem

STSAccessKeyId = os.getenv("OSS_TEST_STS_ID", "")
STSAccessKeySecret = os.getenv("OSS_TEST_STS_KEY", "")
STSArn = os.getenv("OSS_TEST_STS_ARN", "")


def fetch_sts_token(access_key_id: str, access_key_secret: str, role_arn: str):
    clt = client.AcsClient(access_key_id, access_key_secret, "cn-hangzhou")
    req = AssumeRoleRequest.AssumeRoleRequest()

    req.set_accept_format("json")
    req.set_RoleArn(role_arn)
    req.set_RoleSessionName("oss-python-sdk-example")

    body = clt.do_action_with_exception(req)

    j = json.loads(oss2.to_unicode(body))

    access_key_id = j["Credentials"]["AccessKeyId"]
    access_key_secret = j["Credentials"]["AccessKeySecret"]
    security_token = j["Credentials"]["SecurityToken"]

    return access_key_id, access_key_secret, security_token


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_access_key_login(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_bucket_name: str
):
    ossfs.ls(test_bucket_name)


@pytest.mark.parametrize("aio", [False, True])
def test_sts_login(endpoint: str, test_bucket_name: str, aio: bool):
    key, secret, token = fetch_sts_token(STSAccessKeyId, STSAccessKeySecret, STSArn)
    kwargs = {"key": key, "secret": secret, "token": token, "endpoint": endpoint}
    if aio:
        ossfs = AioOSSFileSystem(**kwargs)
    else:
        ossfs = OSSFileSystem(**kwargs)
    ossfs.ls(test_bucket_name)


@pytest.mark.parametrize("aio", [False, True])
def test_set_endpoint(endpoint: str, test_bucket_name: str, monkeypatch, aio: bool):
    key, secret, token = fetch_sts_token(STSAccessKeyId, STSAccessKeySecret, STSArn)
    monkeypatch.delenv("OSS_ENDPOINT")
    kwargs = {"key": key, "secret": secret, "token": token, "endpoint": None}
    if aio:
        ossfs = AioOSSFileSystem(**kwargs)
    else:
        ossfs = OSSFileSystem(**kwargs)
    with pytest.raises(ValueError):
        ossfs.ls(test_bucket_name)
    ossfs.set_endpoint(endpoint)
    ossfs.ls(test_bucket_name)


@pytest.mark.parametrize("aio", [False, True])
def test_env_endpoint(endpoint: str, test_bucket_name: str, monkeypatch, aio: bool):
    key, secret, token = fetch_sts_token(STSAccessKeyId, STSAccessKeySecret, STSArn)
    monkeypatch.setenv("OSS_ENDPOINT", endpoint)
    kwargs = {"key": key, "secret": secret, "token": token, "endpoint": None}
    if aio:
        ossfs = AioOSSFileSystem(**kwargs)
    else:
        ossfs = OSSFileSystem(**kwargs)
    ossfs.ls(test_bucket_name)


@pytest.mark.parametrize("aio", [False, True])
def test_anonymous_login(file_in_anonymous: str, endpoint: str, aio: bool):
    if aio:
        ossfs = AioOSSFileSystem(endpoint=endpoint)
    else:
        ossfs = OSSFileSystem(endpoint=endpoint)
    ossfs.get_object(file_in_anonymous, 1, 100)
