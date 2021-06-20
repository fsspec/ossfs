"""
Three different login test
Sts, anonymous, accesskey
"""
import json
import os

import oss2
from aliyunsdkcore import client
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

from ossfs import OSSFileSystem

STSAccessKeyId = os.getenv("OSS_TEST_STS_ID", "")
STSAccessKeySecret = os.getenv("OSS_TEST_STS_KEY", "")
STSArn = os.getenv("OSS_TEST_STS_ARN", "")


def fetch_sts_token(access_key_id, access_key_secret, role_arn):
    """get token from server
    :return StsToken:
    """
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


def test_access_key_login(ossfs):
    """Test access key login"""
    ossfs.ls("dvc-temp")


def test_sts_login(endpoint):
    """Test sts login"""
    key, secret, token = fetch_sts_token(
        STSAccessKeyId, STSAccessKeySecret, STSArn
    )
    ossfs = OSSFileSystem(
        key=key, secret=secret, token=token, endpoint=endpoint,
    )
    ossfs.ls("dvc-temp")


def test_anonymous_login():
    """test anonymous login"""
    ossfs = OSSFileSystem(endpoint="http://oss-cn-hangzhou.aliyuncs.com")
    ossfs.ls("/dvc-test-anonymous/LICENSE")
