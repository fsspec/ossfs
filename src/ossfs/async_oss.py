"""
Code of AioOSSFileSystem
"""
import copy
import logging
import weakref
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import aiooss2
from aiooss2 import AioBucket, AioService, AnonymousAuth
from aiooss2.http import AioSession
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError
from oss2.exceptions import ClientError

from .base import DEFAULT_POOL_SIZE, BaseOSSFileSystem

if TYPE_CHECKING:
    from oss2.models import (
        ListBucketsResult,
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
            raise ValueError(bucket_name) from err

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
        if not method:
            method = getattr(aiooss2, method_name)
            logger.debug("CALL: %s - %s - %s", method.__name__, args, kwargs)
            out = method(service, *args, **kwargs)
        else:
            logger.debug("CALL: %s - %s - %s", method.__name__, args, kwargs)
            out = await method(*args, **kwargs)
        return out

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
        logger.debug("Get directory listing page for %s", norm_path)
        bucket, key = self.split_path(norm_path)
        if not delimiter or prefix:
            if key:
                prefix = f"{key}/{prefix}"
        else:
            if key:
                prefix = f"{key}/"
            files = []
            async for obj_dict in self._iterdir(
                bucket,
                max_keys=max_items,
                delimiter=delimiter,
                prefix=prefix,
            ):
                files.append(obj_dict)

            self.dircache[norm_path] = files
        result = copy.deepcopy(self.dircache[norm_path])
        return self._post_process_ls_result(path, result)

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

    async def _ls_buckets(self, path, refresh: bool = False) -> List[Dict[str, Any]]:
        if "" not in self.dircache or refresh:
            results: List[Dict[str, Any]] = []
            if isinstance(self._auth, AnonymousAuth):
                logging.warning("cannot list buckets if not logged in")
                return []
            try:
                files: "ListBucketsResult" = await self._call_oss("list_buckets")
            except ClientError:
                # listbucket permission missing
                return []
            file: "SimplifiedBucketInfo"
            for file in files.buckets:
                data: Dict[str, Any] = {}
                data["Key"] = file.name
                data["Size"] = 0
                data["StorageClass"] = "BUCKET"
                data["type"] = "directory"
                self._fill_info(data)
                results.append(data)
            self.dircache[""] = copy.deepcopy(results)
        else:
            results = self.dircache[""]
        return self._post_process_ls_result(path, results)

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
            files = await self._ls_dir(norm_path, refresh)
            if not files and "/" in norm_path:
                files = await self._ls_dir(self._parent(norm_path), refresh=refresh)
                files = [
                    file
                    for file in files
                    if file["name"].strip("/") == norm_path
                    and file["type"] != "directory"
                ]
        else:
            files = await self._ls_buckets(path, refresh)
        return (
            sorted(files, key=lambda i: i["name"])
            if detail
            else sorted(info["name"] for info in files)
        )

    ls = sync_wrapper(_ls)
