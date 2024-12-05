"""
Code of AioOSSFileSystem
"""
import logging
import os
import weakref
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import aiooss2
from aiooss2 import AioBucket, AioService, AnonymousAuth
from aiooss2.http import AioSession
from fsspec.asyn import AsyncFileSystem, _run_coros_in_chunks, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError
from oss2.exceptions import ClientError, OssError, RequestError
from oss2.models import PartInfo

from .base import (
    DEFAULT_BLOCK_SIZE,
    DEFAULT_POOL_SIZE,
    SIMPLE_TRANSFER_THRESHOLD,
    BaseOSSFileSystem,
)
from .exceptions import translate_oss_error
from .utils import as_progress_handler, async_prettify_info_result

if TYPE_CHECKING:
    from aiooss2.models import AioGetObjectResult
    from oss2.models import (
        AppendObjectResult,
        HeadObjectResult,
        InitMultipartUploadResult,
        ListBucketsResult,
        PutObjectResult,
        SimplifiedBucketInfo,
        SimplifiedObjectInfo,
    )

logger = logging.getLogger("ossfs")


class AioOSSFileSystem(BaseOSSFileSystem, AsyncFileSystem):
    # pylint: disable=abstract-method
    """
    A pythonic file-systems interface to OSS (Object Storage Service)
    Base on async operations.

    Examples
    --------
    >>> ossfs = AioOSSFileSystem(anon=False)
    >>> ossfs.ls('my-bucket/')
    ['my-file.txt']

    >>> with ossfs.open('my-bucket/my-file.txt', mode='rb') as f:
    ...     print(f.read())
    b'Hello, world!'
    """

    # pylint:disable=no-value-for-parameter

    protocol = "oss"

    def __init__(
        self,
        psize: int = DEFAULT_POOL_SIZE,
        **kwargs,
    ):
        """
        ----------------------------------------------------------------
        Addition arguments

        Args:
            psize (int, optional): concurrency number of the connections to
            the server. Defaults to DEFAULT_POOL_SIZE.
        """
        super().__init__(**kwargs)
        self._psize = psize
        self._session: Optional["AioSession"] = None

    __init__.__doc__ = (
        BaseOSSFileSystem.__init__.__doc__ + __init__.__doc__  # type: ignore
    )

    def _get_bucket(
        self, bucket_name: str, connect_timeout: Optional[int] = None
    ) -> AioBucket:
        """
        get the new aio bucket instance
        """
        if self._endpoint is None:
            raise ValueError("endpoint is required")
        try:
            return AioBucket(
                auth=self._auth,
                endpoint=self._endpoint,
                bucket_name=bucket_name,
                connect_timeout=connect_timeout,
                session=self._session,
                app_name="ossfs",
            )
        except ClientError as err:
            raise FileNotFoundError(bucket_name) from err

    async def set_session(self, refresh: bool = False):
        """Establish a connection session object.
        Returns
        -------
        Session to be closed later with await .close()
        """
        logger.debug("Connect AioSession instance")
        if self._session is None or self._session.closed or refresh:
            if self._session is None:
                self._session = AioSession(self._psize)
            await self._session.__aenter__()  # pylint: disable=unnecessary-dunder-call
            # the following actually closes the aiohttp connection; use of privates
            # might break in the future, would cause exception at gc time
            if not self.asynchronous:
                weakref.finalize(self, self.close_session)
        return

    def close_session(self):
        """Close a connection session object."""
        if self._session is None or self._session.closed:
            return
        if self.loop is not None and self.loop.is_running():
            try:
                sync(self.loop, self._session.close, timeout=0.1)
                return
            except FSTimeoutError:
                pass

    async def _call_oss(
        self,
        method_name: str,
        *args,
        bucket: Optional[str] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ):
        if self._endpoint is None:
            raise ValueError("endpoint is required")
        await self.set_session()
        if bucket:
            service: Union[AioService, AioBucket] = self._get_bucket(bucket, timeout)
        else:
            service = AioService(
                auth=self._auth,
                endpoint=self._endpoint,
                session=self._session,
                connect_timeout=timeout,
                app_name="ossfs",
            )
        method = getattr(service, method_name, None)
        try:
            if not method:
                method = getattr(aiooss2, method_name)
                logger.debug("CALL: %s - %s - %s", method.__name__, args, kwargs)
                if method_name =="resumable_upload":
                    out = await method(service, *args, **kwargs)
                else:
                    out = method(service, *args, **kwargs)
            else:
                logger.debug("CALL: %s - %s - %s", method.__name__, args, kwargs)
                out = await method(*args, **kwargs)
            return out
        except (RequestError, OssError) as err:
            error = err
        raise translate_oss_error(error) from error

    async def _ls_dir(  # pylint: disable=too-many-arguments
        self,
        path: str,
        refresh: bool = False,
        max_items: int = 100,
        delimiter: str = "/",
        prefix: str = "",
    ):
        norm_path = path.strip("/")
        if norm_path in self.dircache and not refresh and not prefix and delimiter:
            return self.dircache[norm_path]
        logger.debug("Get directory listing for %s", norm_path)
        bucket, key = self.split_path(norm_path)
        prefix = prefix or ""
        if key:
            prefix = f"{key}/{prefix}"
        files = []
        async for obj_dict in self._iterdir(
            bucket,
            max_keys=max_items,
            delimiter=delimiter,
            prefix=prefix,
        ):
            files.append(obj_dict)

        if not prefix and delimiter == "/":
            self.dircache[norm_path] = files
        return files

    async def _iterdir(
        self,
        bucket: str,
        max_keys: int = 100,
        delimiter: str = "/",
        prefix: str = "",
    ):
        """Iterate asynchronously over files and directories under `prefix`.

        The contents are yielded in arbitrary order as info dicts.
        """
        response = await self._call_oss(
            "AioObjectIterator",
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        obj: "SimplifiedObjectInfo"
        async for obj in response:
            data = self._transfer_object_info_to_dict(bucket, obj)
            yield data

    async def _ls_buckets(self, refresh: bool = False) -> List[Dict[str, Any]]:
        if "" not in self.dircache or refresh:
            if isinstance(self._auth, AnonymousAuth):
                logging.warning("cannot list buckets if not logged in")
                return []
            results: List[Dict[str, Any]] = []
            try:
                files: "ListBucketsResult" = await self._call_oss("list_buckets")
            except ClientError:
                # listbucket permission missing
                return []
            file: "SimplifiedBucketInfo"
            for file in files.buckets:
                data: Dict[str, Any] = {}
                data["name"] = file.name
                data["size"] = 0
                data["type"] = "directory"
                results.append(data)
            self.dircache[""] = results
        else:
            results = self.dircache[""]
        return results

    @async_prettify_info_result
    async def _ls(self, path: str, detail: bool = True, **kwargs):
        """List files in given bucket, or list of buckets.

        Listing is cached unless `refresh=True`.

        Note: only your buckets associated with the login will be listed by
        `ls('')`, not any public buckets (even if already accessed).

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        refresh : bool (=False)
            if False, look in local cache for file details first
        """
        refresh = kwargs.pop("refresh", False)
        norm_path = self._strip_protocol(path).strip("/")
        if norm_path != "":
            files = await self._ls_dir(path, refresh)
            if not files and "/" in norm_path:
                files = await self._ls_dir(self._parent(path), refresh=refresh)
                files = [
                    file
                    for file in files
                    if file["name"].strip("/") == norm_path
                    and file["type"] != "directory"
                ]
        else:
            files = await self._ls_buckets(refresh)
        return files

    @async_prettify_info_result
    async def _info(self, path: str, **kwargs):
        norm_path = self._strip_protocol(path).lstrip("/")
        if norm_path == "":
            result = {"name": path, "size": 0, "type": "directory"}
            return result
        bucket, key = self.split_path(norm_path)
        self._get_bucket(bucket)
        refresh = kwargs.pop("refresh", False)
        if not refresh:
            out = self._ls_from_cache(norm_path)
            if out is not None:
                out = [o for o in out if o["name"].strip("/") == norm_path]
                if out:
                    result = out[0]
                else:
                    result = {"name": norm_path, "size": 0, "type": "directory"}
                return result

        if key:
            try:
                obj_out: "HeadObjectResult" = await self._call_oss(
                    "head_object",
                    key,
                    bucket=bucket,
                )
                result = {
                    "LastModified": obj_out.last_modified,
                    "size": obj_out.content_length,
                    "name": path,
                    "type": "file",
                }
                return result
            except (PermissionError, FileNotFoundError):
                pass
            # We check to see if the path is a directory by attempting to list its
            # contexts. If anything is found, it is indeed a directory
            try:
                ls_out = await self._call_oss(
                    "AioObjectIterator",
                    bucket=bucket,
                    prefix=key.rstrip("/") + "/",
                    delimiter="/",
                    max_keys=100,
                )
                try:
                    async for _ in ls_out:
                        return {
                            "size": 0,
                            "name": path,
                            "type": "directory",
                        }
                except OssError as err:
                    raise translate_oss_error(err) from err
            except (PermissionError, FileNotFoundError):
                pass

        else:
            for bucket_info in await self._ls_buckets():
                if bucket_info["name"] == norm_path.rstrip("/"):
                    return {
                        "size": 0,
                        "name": path,
                        "type": "directory",
                    }
        raise FileNotFoundError(path)

    def _cache_result_analysis(self, norm_path: str, parent: str) -> bool:
        if norm_path in self.dircache:
            for file_info in self.dircache[norm_path]:
                # For files the dircache can contain itself.
                # If it contains anything other than itself it is a directory.
                if file_info["name"] != norm_path:
                    return True
            return False

        for file_info in self.dircache[parent]:
            if file_info["name"] == norm_path:
                # If we find ourselves return whether we are a directory
                return file_info["type"] == "directory"
        return False

    async def _isdir(self, path: str) -> bool:
        norm_path = self._strip_protocol(path).strip("/")
        # Send buckets to super
        if norm_path == "":
            return True
        if "/" not in norm_path:
            for bucket_info in await self._ls_buckets():
                if bucket_info["name"] == norm_path:
                    return True
            return False

        parent = self._parent(norm_path)
        if norm_path in self.dircache or parent in self.dircache:
            return self._cache_result_analysis(norm_path, parent)

        # This only returns things within the path and NOT the path object itself
        try:
            return bool(await self._ls_dir(norm_path))
        except FileNotFoundError:
            return False

    async def _put_file(self, lpath: str, rpath: str, **kwargs):
        bucket, key = self.split_path(rpath)
        if os.path.isdir(lpath):
            if key:
                # don't make remote "directory"
                return
            await self._mkdir(lpath)
        else:
            callback = as_progress_handler(kwargs.pop("callback", None))
            if os.path.getsize(lpath) >= SIMPLE_TRANSFER_THRESHOLD:
                await self._call_oss(
                    "resumable_upload",
                    bucket=bucket,
                    key=key,
                    filename=lpath,
                    progress_callback=callback,
                )
            else:
                await self._call_oss(
                    "put_object_from_file",
                    bucket=bucket,
                    key=key,
                    filename=lpath,
                    progress_callback=callback,
                )

        self.invalidate_cache(self._parent(rpath))

    async def _get_file(self, rpath: str, lpath: str, **kwargs):
        """
        Copy single remote file to local
        """
        bucket, key = self.split_path(rpath)
        if await self._isdir(rpath):
            # don't make local "directory"
            return
        callback = as_progress_handler(kwargs.pop("callback", None))
        if await self._size(rpath) >= SIMPLE_TRANSFER_THRESHOLD:
            await self._call_oss(
                "resumable_download",
                bucket=bucket,
                key=key,
                filename=lpath,
                progress_callback=callback,
            )
        else:
            await self._call_oss(
                "get_object_to_file",
                bucket=bucket,
                key=key,
                filename=lpath,
                progress_callback=callback,
                **kwargs,
            )

    @async_prettify_info_result
    async def _find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,  # pylint: disable=unused-argument
        **kwargs,
    ):
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
        prefix: str
            Only return files that match ``^{path}/{prefix}`` (if there is an
            exact match ``filename == {path}/{prefix}``, it also will be included)
        """
        out = {}
        prefix = kwargs.pop("prefix", "")
        path = self._verify_find_arguments(path, maxdepth, withdirs, prefix)
        if prefix:
            for info in await self._ls_dir(path, delimiter="", prefix=prefix):
                out.update({info["name"]: info})
        else:
            async for _, dirs, files in self._walk(path, maxdepth, detail=True):
                if withdirs:
                    files.update(dirs)
                out.update({info["name"]: info for _, info in files.items()})
            if await self._isfile(path) and path not in out:
                # walk works on directories, but find should also return [path]
                # when path happens to be a file
                out[path] = {}
        names = sorted(out)
        return {name: out[name] for name in names}

    async def _bulk_delete(self, pathlist, **kwargs):
        """
        Remove multiple keys with one call

        Parameters
        ----------
        pathlist : list(str)
            The keys to remove, must all be in the same bucket.
            Must have 0 < len <= 1000
        """
        if not pathlist:
            return
        bucket, key_list = self._get_batch_delete_key_list(pathlist)
        await self._call_oss("batch_delete_objects", key_list, bucket=bucket)

    async def _rm_file(self, path: str, **kwargs):
        bucket, key = self.split_path(path)
        await self._call_oss("delete_object", bucket=bucket, key=key)
        self.invalidate_cache(self._parent(path))

    async def _rm(self, path, recursive=False, batch_size=1000, **kwargs):
        if isinstance(path, list):
            for file in path:
                await self._rm(file)
            return

        paths = await self._expand_path(path, recursive=recursive)
        await _run_coros_in_chunks(
            [
                self._bulk_delete(paths[i : i + batch_size])
                for i in range(0, len(paths), batch_size)
            ],
            batch_size=3,
            nofiles=True,
        )

    async def _checksum(self, path, refresh=True):
        """
        Unique value for current version of file

        If the checksum is the same from one moment to another, the contents
        are guaranteed to be the same. If the checksum changes, the contents
        *might* have changed.

        Parameters
        ----------
        path : string/bytes
            path of file to get checksum for
        refresh : bool (=False)
            if False, look in local cache for file details first

        """
        return sha256(
            (
                str(await self._ukey(path))
                + str(await self._info(path, refresh=refresh))
            ).encode()
        ).hexdigest()

    checksum = sync_wrapper(_checksum)

    async def _ukey(self, path: str):
        """Hash of file properties, to tell if it has changed"""
        bucket_name, obj_name = self.split_path(path)
        obj_stream = await self._call_oss("get_object", obj_name, bucket=bucket_name)
        return obj_stream.server_crc

    checksum = sync_wrapper(_checksum)

    async def _cp_file(self, path1: str, path2: str, **kwargs):
        """Copy file between locations on OSS.

        preserve_etag: bool
            Whether to preserve etag while copying. If the file is uploaded
            as a single part, then it will be always equalivent to the md5
            hash of the file hence etag will always be preserved. But if the
            file is uploaded in multi parts, then this option will try to
            reproduce the same multipart upload while copying and preserve
            the generated etag.
        """
        bucket2, key2 = self.split_path(path2)
        bucket1, key1 = self.split_path(path1)
        self.invalidate_cache(self._parent(path2))
        if bucket1 != bucket2:
            tempdir = "." + self.ukey(path1)
            await self._get_file(path1, tempdir, **kwargs)
            await self._put_file(tempdir, path2, **kwargs)
            os.remove(tempdir)
        else:
            connect_timeout = kwargs.pop("connect_timeout", None)
            await self._call_oss(
                "copy_object",
                bucket1,
                key1,
                key2,
                bucket=bucket1,
                timeout=connect_timeout,
            )

    async def _append_object(self, path: str, location: int, value: bytes) -> int:
        """
        Append bytes to the object
        """
        bucket, key = self.split_path(path)
        result: "AppendObjectResult" = await self._call_oss(
            "append_object",
            key,
            location,
            value,
            bucket=bucket,
        )
        return result.next_position

    append_object = sync_wrapper(_append_object)

    async def _get_object(self, path: str, start: int, end: int) -> bytes:
        """
        Return object bytes in range
        """
        headers = {"x-oss-range-behavior": "standard"}
        bucket, key = self.split_path(path)
        object_stream: "AioGetObjectResult" = await self._call_oss(
            "get_object",
            key,
            bucket=bucket,
            byte_range=(start, end),
            headers=headers,
        )
        results = b""
        while True:
            result = await object_stream.read()
            if result:
                results += result
            else:
                break
        return results

    get_object = sync_wrapper(_get_object)

    async def _pipe_file(self, path: str, value: Union[str, bytes], **kwargs):
        bucket, key = self.split_path(path)
        self.invalidate_cache(path)
        block_size = kwargs.get("block_size", DEFAULT_BLOCK_SIZE)
        # 5 GB is the limit for an OSS PUT
        if len(value) < min(5 * 2**30, 2 * block_size):
            await self._call_oss("put_object", key, value, bucket=bucket, **kwargs)
            return
        init_multi_part_upload_result: "InitMultipartUploadResult" = (
            await self._call_oss("init_multipart_upload", key, bucket=bucket, **kwargs)
        )
        parts: List["PartInfo"] = []
        for i, off in enumerate(range(0, len(value), block_size)):
            part_number = i + 1
            value_block = value[off : off + block_size]
            put_object_result: "PutObjectResult" = await self._call_oss(
                "upload_part",
                key,
                init_multi_part_upload_result.upload_id,
                part_number,
                value_block,
                bucket=bucket,
            )
            parts.append(
                PartInfo(
                    part_number,
                    put_object_result.etag,
                    size=len(value_block),
                    part_crc=put_object_result.crc,
                )
            )
        await self._call_oss(
            "complete_multipart_upload",
            key,
            init_multi_part_upload_result.upload_id,
            parts,
            bucket=bucket,
        )

    async def _cat_file(self, path: str, start=None, end=None, **kwargs):
        bucket, key = self.split_path(path)
        object_stream: "AioGetObjectResult" = await self._call_oss(
            "get_object",
            bucket=bucket,
            key=key,
            byte_range=(start, end),
            **kwargs,
        )

        results = b""
        while True:
            result = await object_stream.read()
            if not result:
                break
            results += result
        return results

    async def _modified(self, path: str):
        """Return the modified timestamp of a file as a datetime.datetime"""
        bucket_name, obj_name = self.split_path(path)
        if not obj_name or await self._isdir(path):
            raise NotImplementedError("bucket has no modified timestamp")
        object_meta = await self._call_oss(
            "get_object_meta", obj_name, bucket=bucket_name
        )
        return int(
            datetime.strptime(
                object_meta.headers["Last-Modified"],
                "%a, %d %b %Y %H:%M:%S %Z",
            ).timestamp()
        )

    modified = sync_wrapper(_modified)
