"""
Code of OSSFileSystem and OSSFile
"""
import logging
import os
import re
from hashlib import sha256
from typing import Dict, List, Optional, Tuple, Union

import oss2
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import stringify_path

logger = logging.getLogger("")


def _parse_oss_url(url: str) -> dict:
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


class OSSFileSystem(AbstractFileSystem):
    """
    A pythonic file-systems interface to OSS (Object Storage Service)
    """

    tempdir = "/tmp"
    protocol = "oss"

    def __init__(
        self,
        key: Optional[str] = "",
        secret: Optional[str] = "",
        token: Optional[str] = None,
        endpoint: Optional[str] = None,
        default_cache_type: Optional[str] = "bytes",
        **kwargs,  # pylint: disable=too-many-arguments
    ):
        """
        Parameters
        ----------
        key : string (None)
            If not anonymous, use this access key ID, if specified
        secret : string (None)
            If not anonymous, use this secret access key, if specified
        token : string (None)
            If not anonymous, use this security token, if specified
        endpoint : string (None)
            Defualt endpoints of the fs
            Endpoints are the adderss where OSS locate
            like: http://oss-cn-hangzhou.aliyuncs.com or
                        https://oss-me-east-1.aliyuncs.com
        """
        if token:
            self._auth = oss2.StsAuth(key, secret, token)
        elif key:
            self._auth = oss2.Auth(key, secret)
        else:
            self._auth = oss2.AnonymousAuth()
        self._endpoint = endpoint
        self._default_cache_type = default_cache_type
        super().__init__(**kwargs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,  # pylint: disable=too-many-arguments
    ):
        """
        Open a file for reading or writing.
        Parameters
        ----------
        path: str
            File location
        mode: str
            'rb', 'wb', etc.
        autocommit: bool
            If False, writes to temporary file that only gets put in final
            location upon commit
        kwargs
        Returns
        -------
        OSSFile instance
        """
        cache_type = kwargs.pop("cache_type", self._default_cache_type)
        return OSSFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            cache_type=cache_type,
            **kwargs,
        )

    @classmethod
    def _strip_protocol(cls, path: Union[str, List[str]]):
        """Turn path from fully-qualified to file-system-specifi
        Parameters
        ----------
        path : string
            Input path, like
            `http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject`
        Examples
        --------
        >>> _strip_protocol(
            "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
            )
        ('mybucket/myobject')
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        path = _parse_oss_url(path)["path"]
        path = path.rstrip("/")
        return path or cls.root_marker

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict:
        """get arguments from urls
        Parameters
        ----------
        path : string
            Input path, like
            `http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject`
        Examples
        --------
        >>> _get_kwargs_from_urls(
            "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
            )
        {'path': mybucket/myobject, 'bucket': 'mybucket',
        'object': 'myobject', 'endpoint': 'http://oss-cn-hangzhou.aliyun'}
        """
        return _parse_oss_url(path)

    def _get_path_info(self, urlpath: str) -> Tuple[str, str, str]:
        """get endpoint, bucket name and object name from urls
        Parameters
        ----------
        path : string
            Input path, like
            `http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject`
        Examples
        --------
        >>> _get_kwargs_from_urls(
            "http://oss-cn-hangzhou.aliyuncs.com/mybucket/myobject"
            )
        ('http://oss-cn-hangzhou.aliyun', 'mybucket',  'myobject', )
        >>> _get_kwargs_from_urls("/mybucket/myobject")
        (self._endpoint, 'mybucket',  'myobject', )
        >>> _get_kwargs_from_urls("/mybucket")
        (self._endpoint, 'mybucket', '', )
        """
        url_info = self._get_kwargs_from_urls(urlpath)
        endpoint = url_info["endpoint"]
        endpoint = endpoint if endpoint else self._endpoint
        bucket_name = url_info["bucket"]
        obj_name = url_info["object"]
        return endpoint, bucket_name, obj_name

    def _ls_root(self, endpoint: str) -> List[Dict]:
        infos = []
        service = oss2.Service(self._auth, endpoint)
        for bucket in oss2.BucketIterator(service):
            infos.append(
                {
                    "name": bucket.name,
                    "type": "directory",
                    "size": 0,
                    "StorageClass": "BUCKET",
                }
            )
        return infos

    def _ls_bucket(
        self, endpoint: str, bucket_name: str, obj_name: str
    ) -> List[Dict]:
        infos = []
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        for obj in oss2.ObjectIterator(bucket, prefix=obj_name, delimiter="/"):
            data = {
                "name": bucket_name + "/" + obj.key,
                "type": "file",
                "size": obj.size,
                "StorageClass": "OBJECT",
                "LastModified": obj.last_modified,
            }
            if obj.is_prefix():
                data["type"] = "directory"
                data["size"] = 0
            infos.append(data)
        return infos

    def ls(self, path, detail=True, **kwargs):
        endpoint, bucket_name, obj_name = self._get_path_info(path)

        if not bucket_name:
            infos = self._ls_root(endpoint)
        else:
            try:
                infos = self._ls_bucket(endpoint, bucket_name, obj_name)
            except oss2.exceptions.NoSuchBucket:
                infos = []

        if not infos and not self.exists(path):
            raise FileNotFoundError(path)

        if detail:
            return sorted(infos, key=lambda i: i["name"])
        return sorted(info["name"] for info in infos)

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        obj_name = obj_name.rstrip("/")
        if bucket.object_exists(obj_name):
            return True

        obj_name = obj_name + "/"
        ls_result = self._ls_bucket(endpoint, bucket, obj_name)
        return bool(ls_result)

    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        try:
            obj_stream = bucket.get_object(obj_name)
        except oss2.exceptions.NoSuchKey as err:
            raise FileNotFoundError(path) from err
        return obj_stream.server_crc

    def checksum(self, path):
        """Unique value for current version of file

        If the checksum is the same from one moment to another, the contents
        are guaranteed to be the same. If the checksum changes, the contents
        *might* have changed.

        This should normally be overridden; default will probably capture
        creation/modification timestamp (which would be good) or maybe
        access timestamp (which would be bad)
        """
        return sha256(
            (str(self.ukey(path)) + str(self.info(path))).encode()
        ).hexdigest()

    def cp_file(self, path1, path2, **kwargs):
        """
        Copy within two locations in the filesystem
        # todo: big file optimization
        """
        endpoint1, bucket_name1, obj_name1 = self._get_path_info(path1)
        endpoint2, bucket_name2, obj_name2 = self._get_path_info(path2)
        if endpoint1 != endpoint2 or bucket_name1 != bucket_name2:
            tempdir = "." + self.ukey(path1)
            self.get_file(path1, tempdir)
            self.put_file(tempdir, path2)
            os.remove(tempdir)
        else:
            bucket = oss2.Bucket(self._auth, endpoint1, bucket_name1)
            bucket.copy_object(bucket_name1, obj_name1, obj_name2)

    def _rm(self, path: Union[str, List[str]]):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        """
        if isinstance(path, list):
            for file in path:
                self._rm(file)
        if not self.exists(path):
            return
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        bucket.delete_object(obj_name)

    def get_file(self, rpath, lpath, **kwargs):
        """
        Copy single remote file to local
        # todo optimization for file larger than 5GB
        """
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            endpoint, bucket_name, obj_name = self._get_path_info(lpath)
            bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
            bucket.get_object_to_file(obj_name, rpath)

    def put_file(self, lpath, rpath, **kwargs):
        """
        Copy single file to remote
        # todo optimization for file larger than 5GB
        """
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            endpoint, bucket_name, obj_name = self._get_path_info(lpath)
            bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
            bucket.put_object_from_file(obj_name, rpath)

    def makedirs(self, path, exist_ok=False):
        """Recursively make directories

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If True, will error if the target already exists
        """
        if self.exists(path):
            if exist_ok:
                return
            raise FileExistsError(path)
        path = path.rstrip("/") + "/"
        self.touch(path)

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        raise NotImplementedError("OSS has no created timestamp")

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        simplifiedmeta = bucket.get_object_meta(obj_name)
        return simplifiedmeta.headers["Content-Length"]

    def append_object(self, path: str, location: int, value: bytes) -> int:
        """
        Append bytes to the object
        """
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        result = bucket.append_object(obj_name, location, value)
        return result.next_position

    def get_object(self, path: str, start: int, end: int) -> bytes:
        """
        Return object bytes in range
        """
        headers = {"x-oss-range-behavior": "standard"}
        endpoint, bucket_name, obj_name = self._get_path_info(path)
        bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
        try:
            object_stream = bucket.get_object(
                obj_name, byte_range=(start, end), headers=headers
            )
        except oss2.exceptions.ServerError as err:
            raise err
        return object_stream.read()


class OSSFile(AbstractBufferedFile):
    """A file living in OSSFileSystem"""

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload
        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        self.location = self.fs.append_object(
            self.path, self.location, self.buffer.getvalue()
        )
        return True

    def _initiate_upload(self):
        """ Create remote file/upload """
        if "a" in self.mode:
            self.location = self.size
        elif "w" in self.mode:
            # create empty file to append to
            self.location = 0
            self.fs.rm_file(self.path)

    def _fetch_range(self, start, end):
        """
        Get the specified set of bytes from remote
        Parameters
        ==========
        start: int
        end: int
        """
        start = max(start, 0)
        end = min(self.size, end)
        if start >= end or start >= self.size:
            return b""
        return self.fs.get_object(self.path, start, end)
