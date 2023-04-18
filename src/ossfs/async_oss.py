"""
Code of AioOSSFileSystem
"""
import logging
from typing import Optional

from aiooss2 import AioBucket
from aiooss2.http import AioSession
from fsspec.asyn import AsyncFileSystem
from oss2.exceptions import ClientError

from .base import DEFAULT_POOL_SIZE, BaseOSSFileSystem

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
            the server. Defaults to DEFAULT_CONCURRENCY_NUMBER.
        """
        super().__init__(**kwargs)
        self._session = AioSession(psize)

    __init__.__doc__ = (
        BaseOSSFileSystem.__init__.__doc__ + __init__.__doc__  # type: ignore
    )

    def _get_bucket(
        self, bucket_name: str, connect_timeout: Optional[int] = None
    ) -> AioBucket:
        """
        get the new aio bucket instance
        """
        if not self._endpoint:
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
