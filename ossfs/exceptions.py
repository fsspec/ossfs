"""OSS error codes adapted into more natural Python ones.
Adapted from:
    https://error-center.alibabacloud.com/status/product/Oss?spm=a2c63.p38356.879954.5.3e172c31eo6sN9
"""

import errno

from oss2.exceptions import OssError

ERROR_CODE_TO_EXCEPTION = {
    "NoSuchBucket": FileNotFoundError,
    "NoSuchKey": FileNotFoundError,
    "AccessDenied": PermissionError,
}


def translate_boto_error(
    error: OssError, *args, message=None, set_cause=True, **kwargs
):
    """Convert a ClientError exception into a Python one.
    Parameters
    ----------
    error : oss2.exceptions.OssError
        The exception returned by the OSS Server.
    message : str
        An error message to use for the returned exception. If not given, the
        error message returned by the server is used instead.
    set_cause : bool
        Whether to set the __cause__ attribute to the previous exception if the
        exception is translated.
    *args, **kwargs :
        Additional arguments to pass to the exception constructor, after the
        error message. Useful for passing the filename arguments to
        ``IOError``.
    Returns
    -------
    An instantiated exception ready to be thrown. If the error code isn't
    recognized, an IOError with the original error message is returned.
    """
    if not isinstance(error, OssError):
        # not a oss error:
        return error
    code = error.code
    print("error code", code)
    constructor = ERROR_CODE_TO_EXCEPTION.get(code)
    if constructor:
        if not message:
            message = error.message
        custom_exc = constructor(message, *args, **kwargs)
    else:
        # No match found, wrap this in an IOError with the appropriate message.
        custom_exc = IOError(errno.EIO, message or str(error), *args)

    if set_cause:
        custom_exc.__cause__ = error
    return custom_exc
