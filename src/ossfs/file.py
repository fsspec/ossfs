"""
Code of OSSFileSystem and OSSFile
"""

import logging
from typing import TYPE_CHECKING, Union

from fsspec.spec import AbstractBufferedFile

if TYPE_CHECKING:
    from .async_oss import AioOSSFileSystem
    from .core import OSSFileSystem


logger = logging.getLogger("ossfs")
logging.getLogger("oss2").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class OSSFile(AbstractBufferedFile):
    """A file living in OSSFileSystem"""

    fs: Union["OSSFileSystem", "AioOSSFileSystem"]
    loc: int

    def _upload_chunk(self, final: bool = False) -> bool:  # noqa: ARG002
        """Write one part of a multi-block file upload
        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        self.loc = self.fs.append_object(self.path, self.offset, self.buffer.getvalue())
        return True

    def _initiate_upload(self):
        """Create remote file/upload"""
        if "a" in self.mode:
            self.loc = 0
            if self.fs.exists(self.path):
                self.loc = self.fs.info(self.path)["size"]
        elif "w" in self.mode:
            # create empty file to append to
            self.loc = 0
            if self.fs.exists(self.path):
                self.fs.rm_file(self.path)

    def _fetch_range(self, start: int, end: int) -> bytes:
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
