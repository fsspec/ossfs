"""
OSSFS
----------------------------------------------------------------
A pythonic file-systems interface to OSS (Object Storage Service)
"""
from .async_oss import AioOSSFileSystem
from .core import OSSFileSystem
from .file import OSSFile

__all__ = ["OSSFile", "OSSFileSystem", "AioOSSFileSystem"]
