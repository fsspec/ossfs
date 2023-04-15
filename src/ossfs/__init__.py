"""
OSSFS
----------------------------------------------------------------
A pythonic file-systems interface to OSS (Object Storage Service)
"""
from .core import OSSFileSystem
from .file import OSSFile

__all__ = ["OSSFile", "OSSFileSystem"]
