"""
Code of base class of OSSFileSystem
"""
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from fsspec.spec import AbstractFileSystem
from fsspec.utils import stringify_path
from oss2.auth import AnonymousAuth, Auth, StsAuth
from oss2.defaults import multiget_threshold

logger = logging.getLogger("ossfs")
logging.getLogger("oss2").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)

if TYPE_CHECKING:
    from oss2.models import SimplifiedObjectInfo


DEFAULT_POOL_SIZE = 20
DEFAULT_BLOCK_SIZE = 5 * 2**20
SIMPLE_TRANSFER_THRESHOLD = multiget_threshold


class BaseOSSFileSystem(AbstractFileSystem):
    # pylint: disable=abstract-method

    """
    base class of the ossfs (ossfs) OSS file system access OSS(Object
    Storage Service) as if it were a file system.

    This exposes a filesystem-like API (ls, cp, open, etc.) on top of OSS
    storage.

    Provide credentials with `key` and `secret`, or together with `token`.
    If anonymous leave all these argument empty.

    """

    protocol = "oss"

    def __init__(
        self,
        endpoint: Optional[str] = None,
        key: Optional[str] = None,
        secret: Optional[str] = None,
        token: Optional[str] = None,
        default_cache_type: str = "readahead",
        default_block_size: Optional[int] = None,
        **kwargs,  # pylint: disable=too-many-arguments
    ):
        """
        Parameters
        ----------
        endpoint: string (None)
            Default endpoints of the fs Endpoints are the adderss where OSS
            locate like: http://oss-cn-hangzhou.aliyuncs.com or
            https://oss-me-east-1.aliyuncs.com, Can be changed after the
            initialization.
        key : string (None)
            If not anonymous, use this access key ID, if specified.
        secret : string (None)
            If not anonymous, use this secret access key, if specified.
        token : string (None)
            If not anonymous, use this security token, if specified.
        default_block_size: int (None)
            If given, the default block size value used for ``open()``, if no
            specific value is given at all time. The built-in default is 5MB.
        default_cache_type : string ("readahead")
            If given, the default cache_type value used for ``open()``. Set to "none"
            if no caching is desired. See fsspec's documentation for other available
            cache_type values. Default cache_type is "readahead".

        The following parameters are passed on to fsspec:

        skip_instance_cache: to control reuse of instances
        use_listings_cache, listings_expiry_time, max_paths: to control reuse of
        directory listings
        """
        if token:
            self._auth = StsAuth(key, secret, token)
        elif key:
            self._auth = Auth(key, secret)
        else:
            self._auth = AnonymousAuth()
        self._endpoint = endpoint or os.getenv("OSS_ENDPOINT")
        if self._endpoint is None:
            logger.warning(
                "OSS endpoint is not set, OSSFS could not work properly"
                "without a endpoint, please set it manually with "
                "`ossfs.set_endpoint` later"
            )

        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # passed to fsspec superclass
        super().__init__(**super_kwargs)

        self._default_block_size = default_block_size or DEFAULT_BLOCK_SIZE
        self._default_cache_type = default_cache_type

    def set_endpoint(self, endpoint: str):
        """
        Reset the endpoint for ossfs
        endpoint : string (None)
            Default endpoints of the fs
            Endpoints are the adderss where OSS locate
            like: http://oss-cn-hangzhou.aliyuncs.com or
        """
        if not endpoint:
            raise ValueError("Not a valid endpoint")
        self._endpoint = endpoint

    @classmethod
    def _strip_protocol(cls, path):
        """Turn path from fully-qualified to file-system-specifi
        Parameters
        ----------
        path : Union[str, List[str]]
            Input path, like
            `http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject`
            `oss://mybucket/myobject`
        Examples
        --------
        >>> _strip_protocol(
            "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
            )
        ('/mybucket/myobject')
        >>> _strip_protocol(
            "oss://mybucket/myobject"
            )
        ('/mybucket/myobject')
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path_string: str = stringify_path(path)
        if path_string.startswith("oss://"):
            path_string = path_string[5:]

        parser_re = r"https?://(?P<endpoint>oss.+aliyuncs\.com)(?P<path>/.+)"
        matcher = re.compile(parser_re).match(path_string)
        if matcher:
            path_string = matcher["path"]
        return path_string or cls.root_marker

    def split_path(self, path: str) -> Tuple[str, str]:
        """
        Normalise object path string into bucket and key.
        Parameters
        ----------
        path : string
            Input path, like `/mybucket/path/to/file`
        Examples
        --------
        >>> split_path("/mybucket/path/to/file")
        ['mybucket', 'path/to/file' ]
        >>> split_path("
        /mybucket/path/to/versioned_file?versionId=some_version_id
        ")
        ['mybucket', 'path/to/versioned_file', 'some_version_id']
        """
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, ""
        bucket_name, obj_name = path.split("/", 1)
        return bucket_name, obj_name

    def invalidate_cache(self, path: Optional[str] = None):
        if path is None:
            self.dircache.clear()
        else:
            norm_path: str = self._strip_protocol(path)
            norm_path = norm_path.lstrip("/")
            self.dircache.pop(norm_path, None)
            while norm_path:
                self.dircache.pop(norm_path, None)
                norm_path = self._parent(norm_path)

    def _transfer_object_info_to_dict(
        self, bucket: str, obj: "SimplifiedObjectInfo"
    ) -> Dict:
        data: Dict[str, Any] = {
            "name": "/".join([bucket, obj.key]),
            "type": "file",
            "size": obj.size,
        }
        if obj.last_modified:
            data["LastModified"] = obj.last_modified
        if obj.is_prefix():
            data["type"] = "directory"
            data["size"] = 0
        return data

    def _verify_find_arguments(
        self, path: str, maxdepth: Optional[int], withdirs: bool, prefix: str
    ) -> str:
        path = self._strip_protocol(path)
        bucket, _ = self.split_path(path)
        if not bucket:
            raise ValueError("Cannot traverse all of the buckets")
        if (withdirs or maxdepth) and prefix:
            raise ValueError(
                "Can not specify 'prefix' option alongside "
                "'withdirs'/'maxdepth' options."
            )
        return path

    def _get_batch_delete_key_list(self, pathlist: List[str]) -> Tuple[str, List[str]]:
        buckets: Set[str] = set()
        key_list: List[str] = []
        for path in pathlist:
            bucket, key = self.split_path(path)
            buckets.add(bucket)
            key_list.append(key)
        if len(buckets) > 1:
            raise ValueError("Bulk delete files should refer to only one bucket")
        bucket = buckets.pop()
        if len(pathlist) > 1000:
            raise ValueError("Max number of files to delete in one call is 1000")
        for path in pathlist:
            self.invalidate_cache(self._parent(path))
        return bucket, key_list
