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
from fsspec.implementations.local import make_path_posix
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import other_paths, stringify_path

logger = logging.getLogger("ossfs")
logging.getLogger("oss2").setLevel(logging.CRITICAL)
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


class OSSFileSystem(
    AbstractFileSystem
):  # pylint:disable=too-many-public-methods
    """
    A pythonic file-systems interface to OSS (Object Storage Service)
    """

    tempdir = "/tmp"
    protocol = "oss"
    SIMPLE_TRANSFER_THRESHOLD = oss2.defaults.multiget_threshold

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
        super().__init__(**kwargs)
        if token:
            self._auth = oss2.StsAuth(key, secret, token)
        elif key:
            self._auth = oss2.Auth(key, secret)
        else:
            self._auth = oss2.AnonymousAuth()
        self._endpoint = endpoint
        self._default_cache_type = default_cache_type
        self._session = oss2.Session()

    def _get_bucket(
        self, bucket_name: str, connect_timeout: Optional[int] = None
    ):
        """
        get the new bucket instance
        """
        return oss2.Bucket(
            self._auth,
            self._endpoint,
            bucket_name,
            session=self._session,
            connect_timeout=connect_timeout,
            app_name="ossfs",
        )

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
        bucket = self._get_bucket(bucket_name, connect_timeout)
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

    def _get_object_info_list(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str,
        connect_timeout: int,
    ):
        """
        Wrap oss2.ObjectIterator return values into a
        fsspec form of file info
        """
        bucket = self._get_bucket(bucket_name, connect_timeout)
        infos = []
        for obj in oss2.ObjectIterator(
            bucket, prefix=prefix, delimiter=delimiter
        ):
            data = {
                "name": f"{bucket_name}/{obj.key}",
                "Key": f"{bucket_name}/{obj.key}",
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

    def _ls_dir(
        self,
        path: str,
        delimiter: str = "/",
        prefix: Optional[str] = None,
        connect_timeout: int = None,
    ) -> List[Dict]:
        norm_path = path.strip("/")
        bucket_name, key = self.split_path(norm_path)
        if not prefix:
            prefix = ""
        if key:
            prefix = f"{key}/{prefix}"

        if not delimiter or prefix:
            infos = self._get_object_info_list(
                bucket_name, prefix, delimiter, connect_timeout
            )
        else:
            if norm_path not in self.dircache:
                self.dircache[norm_path] = self._get_object_info_list(
                    bucket_name, prefix, delimiter, connect_timeout
                )
            infos = self.dircache[norm_path]
        if path.startswith("/"):
            for info in infos:
                info["name"] = f'/{info["name"]}'
                info["Key"] = f'/{info["Key"]}'
        return infos

    def ls(self, path, detail=True, **kwargs):
        connect_timeout = kwargs.pop("connect_timeout", 60)
        bucket_name, _ = self.split_path(path)
        if bucket_name:
            infos = self._ls_object(path, connect_timeout)
            if not infos:
                infos = self._ls_dir(path, connect_timeout=connect_timeout)
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
            for info in self._ls_dir(
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
        ls_result = self._ls_dir(dirname, connect_timeout=connect_timeout)
        return bool(ls_result)

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        bucket_name, obj_name = self.split_path(path)
        if not bucket_name:
            return False

        connect_timeout = kwargs.get("connect_timeout", None)
        bucket = self._get_bucket(bucket_name, connect_timeout)
        if not self._bucket_exist(bucket):
            return False

        if not obj_name:
            return True

        if self._object_exists(bucket, obj_name):
            return True

        return self._directory_exists(path, **kwargs)

    @error_decorator
    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        bucket_name, obj_name = self.split_path(path)
        bucket = self._get_bucket(bucket_name)
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
            bucket = self._get_bucket(bucket_name1, connect_timeout)
            bucket.copy_object(bucket_name1, obj_name1, obj_name2)
        self.invalidate_cache(self._parent(path2))

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
            return
        bucket_name, obj_name = self.split_path(path)
        bucket = self._get_bucket(bucket_name)
        bucket.delete_object(obj_name)
        self.invalidate_cache(self._parent(path))

    @error_decorator
    def rm(self, path, recursive=False, maxdepth=None):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """

        if isinstance(path, list):
            for file in path:
                self.rm(file)
            return

        bucket_name, _ = self.split_path(path)
        bucket = self._get_bucket(bucket_name)
        path_expand = self.expand_path(
            path, recursive=recursive, maxdepth=maxdepth
        )
        path_expand = [self.split_path(file)[1] for file in path_expand]

        def chunks(lst: list, num: int):
            for i in range(0, len(lst), num):
                yield lst[i : i + num]

        for files in chunks(path_expand, 1000):
            bucket.batch_delete_objects(files)

        self.invalidate_cache(self._parent(path))

    def get_path(self, rpath, lpath, **kwargs):
        """
        Copy single remote path to local
        """
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            self.get_file(rpath, lpath, **kwargs)

    def get_file(self, rpath, lpath, **kwargs):
        """
        Copy single remote file to local
        """
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            bucket_name, obj_name = self.split_path(rpath)
            connect_timeout = kwargs.pop("connect_timeout", None)
            bucket = self._get_bucket(bucket_name, connect_timeout)
            if self.size(rpath) >= self.SIMPLE_TRANSFER_THRESHOLD:
                oss2.resumable_download(bucket, obj_name, lpath, **kwargs)
            else:
                bucket.get_object_to_file(obj_name, lpath, **kwargs)

    def get(self, rpath, lpath, recursive=False, **kwargs):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        Calls get_file for each source.
        """

        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        rpaths = self.expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        for r_path, l_path in zip(rpaths, lpaths):
            self.get_path(r_path, l_path, **kwargs)

    def put_file(self, lpath, rpath, **kwargs):
        """
        Copy single file to remote
        """
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
        else:
            bucket_name, obj_name = self.split_path(rpath)
            connect_timeout = kwargs.pop("connect_timeout", None)
            bucket = self._get_bucket(bucket_name, connect_timeout)
            if os.path.getsize(lpath) >= self.SIMPLE_TRANSFER_THRESHOLD:
                oss2.resumable_upload(bucket, obj_name, lpath, **kwargs)
            else:
                bucket.put_object_from_file(obj_name, lpath, **kwargs)
        self.invalidate_cache(self._parent(rpath))

    @error_decorator
    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if obj_name:
            raise NotImplementedError("OSS has no created timestamp")
        bucket = self._get_bucket(bucket_name)
        timestamp = bucket.get_bucket_info().creation_date
        return datetime.fromtimestamp(timestamp)

    @error_decorator
    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if not obj_name or self.isdir(path):
            raise NotImplementedError("bucket has no modified timestamp")
        bucket = self._get_bucket(bucket_name)
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
        bucket = self._get_bucket(bucket_name)
        result = bucket.append_object(obj_name, location, value)
        return result.next_position

    @error_decorator
    def get_object(self, path: str, start: int, end: int) -> bytes:
        """
        Return object bytes in range
        """
        headers = {"x-oss-range-behavior": "standard"}
        bucket_name, obj_name = self.split_path(path)
        bucket = self._get_bucket(bucket_name)
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
            self.invalidate_cache(self._parent(path))

    @dynamic_block_size
    def cat_file(self, path, start=None, end=None, **kwargs):
        """ Get the content of a file """
        return super().cat_file(path, start, end, **kwargs)

    @error_decorator
    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file"""
        bucket_name, obj_name = self.split_path(path)
        bucket = self._get_bucket(bucket_name)
        bucket.put_object(obj_name, value, **kwargs)
        self.invalidate_cache(self._parent(path))

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            self.dircache.pop(path, None)
            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)


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
