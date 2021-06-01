"""
Utils used for OSSFS
"""
import re


def parse_oss_url(url: str) -> dict:
    """parse urls from urls
    Parameters
    ----------
    path : string
        Input path, like
        `http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject`
    Examples
    --------
    >>> _get_kwargs_from_urls(
        "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject")
    {'bucket': 'mybucket', 'object': 'myobject',
    'endpoint': 'http://oss-cn-hangzhou.aliyun.com'}
    """
    out = {}

    parser_re = r"https?://(?P<endpoint>oss.+aliyuncs\.com)(?P<path>/.+)"
    matcher = re.compile(parser_re).match(url)
    if matcher:
        url = matcher["path"]
        out["endpoint"] = matcher["endpoint"]
    else:
        out["endpoint"] = ""

    out["path"] = url
    url = url.lstrip("/")
    if "/" not in url:
        bucket_name = url
        obj_name = ""
    else:
        bucket_name, obj_name = url.split("/", 1)
    out["object"] = obj_name
    out["bucket"] = bucket_name

    return out
