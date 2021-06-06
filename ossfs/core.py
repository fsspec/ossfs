"""
Code of OSSFileSystem and OSSFile
"""
import logging
import os
import re
from datetime import datetime
from hashlib import sha256
from typing import Dict, List, Optional, Tuple, Union

import oss2
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import stringify_path

logger = logging.getLogger("")


def error_decorator(func):
    """
    Warp oss exceptions to file system exceptions
    """

    def new_func(self, path, *args, **kwargs):
        try:
            result = func(self, path, *args, **kwargs)
        except (
            oss2.exceptions.NoSuchBucket,
            oss2.exceptions.NoSuchKey,
        ) as err:
            raise FileNotFoundError(path) from err
        return result

    return new_func


class OSSFileSystem(AbstractFileSystem):
    """
    A pythonic file-systems interface to OSS (Object Storage Service)
    """

    tempdir = "/tmp"
    protocol = "oss"

    def __init__(
        self,
        endpoint: str,
        key: Optional[str] = None,
        secret: Optional[str] = None,
        token: Optional[str] = None,
        default_cache_type: Optional[str] = "readahead",
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
        parser_re = r"https?://(?P<endpoint>oss.+aliyuncs\.com)(?P<path>/.+)"
        matcher = re.compile(parser_re).match(path)
        if matcher:
            path = path["path"]
        path = path.rstrip("/")
        return path or cls.root_marker

    def _ls_bucket(self) -> List[Dict]:
        infos = []
        service = oss2.Service(self._auth, self._endpoint)
        for bucket in oss2.BucketIterator(service):
            infos.append(
                {
                    "name": bucket.name,
                    "type": "directory",
                    "size": 0,
                    "Size": 0,
                    "StorageClass": "BUCKET",
                    "CreateTime": bucket.creation_date,
                }
            )
        return infos

    def _ls_object(self, bucket_name: str, obj_name: str) -> List[Dict]:
        infos = []
        obj_name.rstrip("/")
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        simplifiedmeta = bucket.get_object_meta(obj_name)
        infos.append(
            {
                "name": bucket_name + "/" + obj_name,
                "type": "file",
                "size": int(simplifiedmeta.headers["Content-Length"]),
                "Size": int(simplifiedmeta.headers["Content-Length"]),
                "StorageClass": "OBJECT",
                "LastModified": simplifiedmeta.headers["Last-Modified"],
            }
        )
        return infos

    def _ls_directory(self, bucket_name: str, directory: str) -> List[Dict]:
        infos = []
        if not directory.endswith("/"):
            directory += "/"
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        for obj in oss2.ObjectIterator(
            bucket, prefix=directory, delimiter="/"
        ):
            data = {
                "name": bucket_name + "/" + obj.key,
                "type": "file",
                "size": obj.size,
                "Size": obj.size,
                "StorageClass": "OBJECT",
                "LastModified": obj.last_modified,
            }
            if obj.is_prefix():
                data["type"] = "directory"
                data["size"] = 0
                data["Size"] = 0
            infos.append(data)
        return infos

    def ls(self, path, detail=True, **kwargs):
        bucket_name, obj_name = self.split_path(path)
        if bucket_name:
            try:
                infos = self._ls_object(bucket_name, obj_name)
            except oss2.exceptions.OssError:
                infos = self._ls_directory(bucket_name, obj_name)
        else:
            if not obj_name:
                raise ValueError(path)
            infos = self._ls_bucket()

        if detail:
            return sorted(infos, key=lambda i: i["name"])
        return sorted(info["name"] for info in infos)

    @staticmethod
    def bucket_exist(bucket: oss2.Bucket):
        """Is the bucket exists"""
        try:
            bucket.get_bucket_info()
        except oss2.exceptions.NoSuchBucket:
            return False
        return True

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        obj_name = obj_name.rstrip("/")
        if not obj_name:
            return self.bucket_exist(bucket)

        if bucket.object_exists(obj_name):
            return True

        obj_name = obj_name + "/"
        ls_result = self._ls_object(bucket_name, obj_name)
        return bool(ls_result)

    @error_decorator
    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        obj_stream = bucket.get_object(obj_name)
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
        bucket_name1, obj_name1 = self.split_path(path1)
        bucket_name2, obj_name2 = self.split_path(path2)
        if bucket_name1 != bucket_name2:
            tempdir = "." + self.ukey(path1)
            self.get_file(path1, tempdir)
            self.put_file(tempdir, path2)
            os.remove(tempdir)
        else:
            bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name1)
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
            raise FileNotFoundError(path)
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        bucket.delete_object(obj_name)

    def get_file(self, rpath, lpath, **kwargs):
        """
        Copy single remote file to local
        # todo optimization for file larger than 5GB
        """
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            bucket_name, obj_name = self.split_path(lpath)
            bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
            bucket.get_object_to_file(obj_name, rpath)

    def put_file(self, lpath, rpath, **kwargs):
        """
        Copy single file to remote
        # todo optimization for file larger than 5GB
        """
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            bucket_name, obj_name = self.split_path(lpath)
            bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
            bucket.put_object_from_file(obj_name, rpath)

    @error_decorator
    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if obj_name:
            raise NotImplementedError("OSS has no created timestamp")
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        timestamp = bucket.get_bucket_info().creation_date
        return datetime.fromtimestamp(timestamp)

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if not obj_name:
            raise NotImplementedError("bucket has no modified timestamp")
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        simplifiedmeta = bucket.get_object_meta(obj_name)
        timestamp = simplifiedmeta.headers["Last-Modified"]
        return datetime.fromtimestamp(timestamp)

    @error_decorator
    def append_object(self, path: str, location: int, value: bytes) -> int:
        """
        Append bytes to the object
        """
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        result = bucket.append_object(obj_name, location, value)
        return result.next_position

    @error_decorator
    def get_object(self, path: str, start: int, end: int) -> bytes:
        """
        Return object bytes in range
        """
        headers = {"x-oss-range-behavior": "standard"}
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        try:
            object_stream = bucket.get_object(
                obj_name, byte_range=(start, end), headers=headers
            )
        except oss2.exceptions.ServerError as err:
            raise err
        return object_stream.read()

    def sign(self, path, expiration=100, **kwargs):
        raise NotImplementedError(
            "Sign is not implemented for this filesystem"
        )


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
        self.loc = self.fs.append_object(
            self.path, self.loc, self.buffer.getvalue()
        )
        return True

    def _initiate_upload(self):
        """ Create remote file/upload """
        if "a" in self.mode:
            self.loc = 0
            if self.fs.exists(self.path):
                self.loc = self.fs.info(self.path)["size"]
        elif "w" in self.mode:
            # create empty file to append to
            self.loc = 0
            if self.fs.exists(self.path):
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
