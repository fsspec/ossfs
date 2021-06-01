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

    security_token = j["Credentials"]["SecurityToken"]

    return security_token


def test_access_key_login(ossfs):
    """Test access key login"""
    ossfs.ls("dvc-temp")


def test_sts_login(endpoint):
    """Test sts login"""
    token = fetch_sts_token(STSAccessKeyId, STSAccessKeySecret, STSArn)
    ossfs = OSSFileSystem(
        key=STSAccessKeySecret,
        secret=STSAccessKeySecret,
        token=token,
        endpoint=endpoint,
    )
    ossfs.ls("dvc-temp")


def test_anonymous_login(endpoint):
    """test anonymous login"""
    ossfs = OSSFileSystem(endpoint=endpoint)
    ossfs.ls("/dvc-test-anonymous")