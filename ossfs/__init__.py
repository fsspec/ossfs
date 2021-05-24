"""
OSSFS
----------------------------------------------------------------
A pythonic file-systems interface to OSS (Object Storage Service)
"""
from .core import OSSFile, OSSFileSystem

__all__ = ["OSSFile", "OSSFileSystem"]
