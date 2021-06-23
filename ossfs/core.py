"""
Code of OSSFileSystem and OSSFile
"""
import logging
import os
import re
from datetime import datetime
from functools import wraps
from hashlib import sha256
from typing import Dict, List, Optional, Tuple, Union

import oss2
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import stringify_path

logger = logging.getLogger("ossfs")
logging.getLogger("oss2").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def dynamic_block_size(func):
    """
    dynamic ajust block size on connection errors
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        retry_count = 0
        block_size = kwargs.pop("block_size", None)
        if not block_size:
            block_size = OSSFile.DEFAULT_BLOCK_SIZE
            user_specified = False
        else:
            user_specified = True

        while True:
            try:
                return func(*args, block_size=block_size, **kwargs)
            except oss2.exceptions.RequestError as error:
                if user_specified or block_size < 2 or retry_count >= 5:
                    raise error
                block_size = block_size // 2
                retry_count += 1

    return wrapper


def error_decorator(func):
    """
    Warp oss exceptions to file system exceptions
    """

    @wraps(func)
    def new_func(self, path, *args, **kwargs):
        try:
            result = func(self, path, *args, **kwargs)
        except (
            oss2.exceptions.NoSuchBucket,
            oss2.exceptions.NoSuchKey,
        ) as err:
            raise FileNotFoundError(path) from err
        except oss2.exceptions.ServerError as err:
            raise ValueError(path) from err
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

    @error_decorator
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
        path = stringify_path(path)
        if path.startswith("oss://"):
            path = path[5:]

        parser_re = r"https?://(?P<endpoint>oss.+aliyuncs\.com)(?P<path>/.+)"
        matcher = re.compile(parser_re).match(path)
        if matcher:
            path = matcher["path"]
        return path or cls.root_marker

    def _ls_bucket(self, connect_timeout) -> List[Dict]:
        service = oss2.Service(
            self._auth,
            endpoint=self._endpoint,
            connect_timeout=connect_timeout,
        )
        infos = []
        for bucket in oss2.BucketIterator(service):
            infos.append(
                {
                    "name": bucket.name,
                    "Key": bucket.name,
                    "type": "directory",
                    "size": 0,
                    "Size": 0,
                    "StorageClass": "BUCKET",
                    "CreateTime": bucket.creation_date,
                }
            )
        return infos

    def _ls_object(self, path: str, connect_timeout) -> List[Dict]:
        bucket_name, obj_name = self.split_path(path)
        bucket = oss2.Bucket(
            self._auth,
            self._endpoint,
            bucket_name,
            connect_timeout=connect_timeout,
        )
        infos = []
        if not self._object_exists(bucket, obj_name):
            return infos
        simplifiedmeta = bucket.get_object_meta(obj_name)
        info = {
            "name": path,
            "Key": path,
            "type": "file",
            "size": int(simplifiedmeta.headers["Content-Length"]),
            "Size": int(simplifiedmeta.headers["Content-Length"]),
            "StorageClass": "OBJECT",
        }
        if "Last-Modified" in simplifiedmeta.headers:
            info["LastModified"] = int(
                datetime.strptime(
                    simplifiedmeta.headers["Last-Modified"],
                    "%a, %d %b %Y %H:%M:%S %Z",
                ).timestamp()
            )
        infos.append(info)

        return infos

    def _ls_directory(
        self,
        path: str,
        delimiter: str = "/",
        prefix: Optional[str] = None,
        connect_timeout: int = None,
    ) -> List[Dict]:
        path = path.rstrip("/") + "/"
        bucket_name, directory = self.split_path(path)
        bucket = oss2.Bucket(
            self._auth,
            self._endpoint,
            bucket_name,
            connect_timeout=connect_timeout,
        )
        infos = []
        bucket_path = (
            f"/{bucket_name}/" if path.startswith("/") else f"{bucket_name}/"
        )
        prefix = (
            directory
            if not prefix
            else "/".join([directory.rstrip("/"), prefix])
        )
        for obj in oss2.ObjectIterator(
            bucket, prefix=prefix, delimiter=delimiter
        ):
            data = {
                "name": bucket_path + obj.key,
                "Key": bucket_path + obj.key,
                "type": "file",
                "size": obj.size,
                "Size": obj.size,
                "StorageClass": "OBJECT",
            }
            if obj.last_modified:
                data["LastModified"] = obj.last_modified
            if obj.is_prefix():
                data["type"] = "directory"
                data["size"] = 0
                data["Size"] = 0
            infos.append(data)
        return infos

    def ls(self, path, detail=True, **kwargs):
        connect_timeout = kwargs.pop("connect_timeout", 60)
        bucket_name, _ = self.split_path(path)
        if bucket_name:
            infos = self._ls_object(path, connect_timeout)
            if not infos:
                infos = self._ls_directory(
                    path, connect_timeout=connect_timeout
                )
        else:
            infos = self._ls_bucket(connect_timeout)

        if not infos:
            raise FileNotFoundError(path)
        if detail:
            return sorted(infos, key=lambda i: i["name"])
        return sorted(info["name"] for info in infos)

    def find(self, path, maxdepth=None, withdirs=False, **kwargs):
        """List all files below path.

        Like posix ``find`` command without conditions

        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        path = self._strip_protocol(path)
        out = {}
        detail = kwargs.pop("detail", False)
        prefix = kwargs.pop("prefix", None)
        if (withdirs or maxdepth) and prefix:
            raise ValueError(
                "Can not specify 'prefix' option alongside "
                "'withdirs'/'maxdepth' options."
            )
        if prefix:
            connect_timeout = kwargs.get("connect_timeout", None)
            for info in self._ls_directory(
                path,
                delimiter="",
                prefix=prefix,
                connect_timeout=connect_timeout,
            ):
                out.update({info["name"]: info})
        else:
            for _, dirs, files in self.walk(
                path, maxdepth, detail=True, **kwargs
            ):
                if withdirs:
                    files.update(dirs)
                out.update(
                    {info["name"]: info for name, info in files.items()}
                )
            if self.isfile(path) and path not in out:
                # walk works on directories, but find should also return [path]
                # when path happens to be a file
                out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        return {name: out[name] for name in names}

    @staticmethod
    def _bucket_exist(bucket: oss2.Bucket):
        """Is the bucket exists"""
        try:
            bucket.get_bucket_info()
        except oss2.exceptions.OssError:
            return False
        return True

    @staticmethod
    def _object_exists(bucket: oss2.Bucket, object_name: str):
        if not object_name:
            return False
        return bucket.object_exists(object_name)

    def _directory_exists(self, dirname: str, **kwargs):
        connect_timeout = kwargs.pop("connect_timeout", None)
        ls_result = self._ls_directory(
            dirname, connect_timeout=connect_timeout
        )
        return bool(ls_result)

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        bucket_name, obj_name = self.split_path(path)
        if not bucket_name:
            return False

        connect_timeout = kwargs.get("connect_timeout", None)
        bucket = oss2.Bucket(
            self._auth,
            self._endpoint,
            bucket_name,
            connect_timeout=connect_timeout,
        )
        if not self._bucket_exist(bucket):
            return False

        if not obj_name:
            return True

        if bucket.object_exists(obj_name):
            return True

        return self._directory_exists(path, **kwargs)

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
            self.get_file(path1, tempdir, **kwargs)
            self.put_file(tempdir, path2, **kwargs)
            os.remove(tempdir)
        else:
            connect_timeout = kwargs.pop("connect_timeout", None)
            bucket = oss2.Bucket(
                self._auth,
                self._endpoint,
                bucket_name1,
                connect_timeout=connect_timeout,
            )
            bucket.copy_object(bucket_name1, obj_name1, obj_name2)

    @error_decorator
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
            bucket_name, obj_name = self.split_path(rpath)
            connect_timeout = kwargs.pop("connect_timeout", None)
            bucket = oss2.Bucket(
                self._auth,
                self._endpoint,
                bucket_name,
                connect_timeout=connect_timeout,
            )
            oss2.resumable_download(bucket, obj_name, lpath, **kwargs)

    def put_file(self, lpath, rpath, **kwargs):
        """
        Copy single file to remote
        # todo optimization for file larger than 5GB
        """
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            bucket_name, obj_name = self.split_path(rpath)
            connect_timeout = kwargs.pop("connect_timeout", None)
            bucket = oss2.Bucket(
                self._auth,
                self._endpoint,
                bucket_name,
                connect_timeout=connect_timeout,
            )
            if self._object_exists(bucket, obj_name):
                raise FileExistsError(rpath)
            oss2.resumable_upload(bucket, obj_name, lpath, **kwargs)

    @error_decorator
    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if obj_name:
            raise NotImplementedError("OSS has no created timestamp")
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        timestamp = bucket.get_bucket_info().creation_date
        return datetime.fromtimestamp(timestamp)

    @error_decorator
    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if not obj_name or self.isdir(path):
            raise NotImplementedError("bucket has no modified timestamp")
        bucket = oss2.Bucket(self._auth, self._endpoint, bucket_name)
        simplifiedmeta = bucket.get_object_meta(obj_name)
        return int(
            datetime.strptime(
                simplifiedmeta.headers["Last-Modified"],
                "%a, %d %b %Y %H:%M:%S %Z",
            ).timestamp()
        )

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

    def touch(self, path, truncate=True, **kwargs):
        """Create empty file, or update timestamp

        Parameters
        ----------
        path: str
            file location
        truncate: bool
            If True, always set file size to 0; if False, update timestamp and
            leave file unchanged, if backend allows this
        """
        if truncate or not self.exists(path):
            with self.open(path, "wb", **kwargs):
                pass

    @dynamic_block_size
    def cat_file(self, path, start=None, end=None, **kwargs):
        """ Get the content of a file """
        return super().cat_file(path, start, end, **kwargs)

    @dynamic_block_size
    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file"""
        with self.open(path, "wb", **kwargs) as f_w:
            f_w.write(value)


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
