"""utils of ossfs"""
import copy
import inspect
from functools import wraps
from typing import Any, Dict, List


def _copy_and_pretify_list(
    path: str, result: List[Dict[str, Any]], detail: bool
) -> List[Dict[str, Any]]:
    result_copy = copy.deepcopy(result)
    if not detail:
        for path_info in result_copy:
            if path.startswith("/"):
                path_info["name"] = f'/{path_info["name"].lstrip("/")}'
            else:
                path_info["name"] = f'{path_info["name"].lstrip("/")}'
        final_results = sorted(info["name"] for info in result_copy)
    else:
        for path_info in result_copy:
            path_info["Size"] = path_info["size"]
            path_info["Key"] = path_info["name"]
            if path.startswith("/"):
                path_info["name"] = f'/{path_info["name"].lstrip("/")}'
                path_info["Key"] = f'/{path_info["Key"].lstrip("/")}'
            else:
                path_info["name"] = f'{path_info["name"].lstrip("/")}'
                path_info["Key"] = f'{path_info["Key"].lstrip("/")}'
        final_results = sorted(result_copy, key=lambda i: i["name"])
    return final_results


def _format_unify(path: str, result, detail: bool):
    if not result:
        return result
    if isinstance(result, dict):
        for _, value in result.items():
            nested = isinstance(value, dict)
            break
        if nested:
            result_list = [path_info for _, path_info in result.items()]
            normed_result = _copy_and_pretify_list(path, result_list, detail)
            if detail:
                return {path_info["name"]: path_info for path_info in normed_result}
            return normed_result

        return _copy_and_pretify_list(path, [result], detail)[0]

    return _copy_and_pretify_list(path, result, detail)


def prettify_info_result(func):
    """Make the return values of `ls` and `info` follows the fsspec's standard
    Examples:
    --------------------------------
    @pretify_info_result
    def ls(path: str, ...)
    """

    @wraps(func)
    def wrapper(ossfs, path: str, *args, **kwargs):
        func_params = inspect.signature(func).parameters
        if "detail" in func_params:
            detail = kwargs.get("detail", func_params["detail"].default)
        else:
            detail = kwargs.get("detail", True)

        result = func(ossfs, path, *args, **kwargs)
        return _format_unify(path, result, detail)

    return wrapper


def async_prettify_info_result(func):
    """Make the return values of async func `ls` and `info` follows the
    fsspec's standard
    Examples:
    --------------------------------
    @async_pretify_info_result
    async def ls(path: str, ...)
    """

    @wraps(func)
    async def wrapper(ossfs, path: str, *args, **kwargs):
        func_params = inspect.signature(func).parameters
        if "detail" in func_params:
            detail = kwargs.get("detail", func_params["detail"].default)
        else:
            detail = kwargs.get("detail", True)
        result = await func(ossfs, path, *args, **kwargs)
        return _format_unify(path, result, detail)

    return wrapper


def as_progress_handler(callback):
    """progress bar handler"""
    if callback is None:
        return None

    sent_total = False

    def progress_handler(absolute_progress, total_size):
        nonlocal sent_total
        if not sent_total:
            callback.set_size(total_size)
            sent_total = True

        callback.absolute_update(absolute_progress)

    return progress_handler
