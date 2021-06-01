"""
Test utils
"""
from ossfs.utils import parse_oss_url


def test_parse_oss_url():
    """
    test url parsing
    """
    url = "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
    result = parse_oss_url(url)
    assert result == {
        "bucket": "mybucket",
        "endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "object": "myobject",
        "path": "/mybucket/myobject",
    }

    url = "/mybucket/myobject"
    result = parse_oss_url(url)
    assert result == {
        "bucket": "mybucket",
        "endpoint": "",
        "object": "myobject",
        "path": "/mybucket/myobject",
    }

    url = "http://oss-cn-hangzhou.aliyuncs.com/mybucket"
    result = parse_oss_url(url)
    assert result == {
        "bucket": "mybucket",
        "endpoint": "oss-cn-hangzhou.aliyuncs.com",
        "object": "",
        "path": "/mybucket",
    }
