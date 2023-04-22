"""
Test all oss object related methods
"""
import inspect

# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
# pylint:disable=consider-using-with
import os
import time
from typing import TYPE_CHECKING, Union

import fsspec
import pytest

from .conftest import NUMBERS, bucket_relative_path

if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.fixture(scope="module", name="test_path")
def file_level_path(test_bucket_name: str, test_directory: str):
    current_file = __file__.rsplit(os.sep, maxsplit=1)[-1]
    return f"/{test_bucket_name}/{test_directory}/{current_file}"


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_info(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    # test file info
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name_foo = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/foo"
    bucket.put_object(bucket_relative_path(object_name_foo), "foo")
    info = ossfs.info(object_name_foo)
    linfo = ossfs.ls(object_name_foo, detail=True)[0]

    assert abs(info.pop("LastModified") - linfo.pop("LastModified")) <= 1
    assert info["size"] == 3
    assert info == linfo

    # test not exist dir
    file_path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    object_name_bar = file_path + "bar"
    with pytest.raises(FileNotFoundError):
        ossfs.info(object_name_bar)
    ossfs.invalidate_cache(file_path)

    # add a new file then we can info this new dir.
    bucket.put_object(bucket_relative_path(object_name_bar), "bar")
    ossfs.info(file_path + "bar")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_checksum(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    # don't make local "directory"
    function_name = inspect.stack()[0][0].f_code.co_name
    object_name = f"{test_path}/{function_name}"

    # change one file, using cache
    bucket.put_object(bucket_relative_path(object_name), "foo")
    checksum = ossfs.checksum(object_name)

    # Test changed contents
    bucket.put_object(bucket_relative_path(object_name), "bar")
    assert checksum != ossfs.checksum(object_name)
    # Test for same contents

    # sleep 1 second to make sure the last modificed changes
    time.sleep(1)
    bucket.put_object(bucket_relative_path(object_name), "foo")
    ossfs.invalidate_cache()
    assert checksum != ossfs.checksum(object_name)

    # Test for nonexistent file
    bucket.delete_object(bucket_relative_path(object_name))
    with pytest.raises(FileNotFoundError):
        ossfs.checksum(object_name)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ls_object(
    # don't make local "directory"
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_path: str,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    assert ossfs.ls(path + "nonexistent") == []
    ossfs.invalidate_cache(path)
    filename = path + "accounts.1.json"
    bucket.put_object(bucket_relative_path(filename), "")
    assert filename in ossfs.ls(path, detail=False)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ls_dir(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file = path + "file"
    bucket.put_object(bucket_relative_path(file), "")
    assert file in ossfs.ls(path, detail=False)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ls_and_touch(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    # don't make local "directory"
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file_a = path + "a"
    file_b = path + "b"
    assert not bucket.object_exists(bucket_relative_path(path))
    ossfs.touch(file_a)
    ossfs.touch(file_b)
    ls_result = ossfs.ls(path, detail=True)
    assert {result["Key"] for result in ls_result} == {file_a, file_b}
    ls_result = ossfs.ls(path, detail=False)
    assert set(ls_result) == {file_a, file_b}


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_isfile(
    # don't make local "directory"
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_path: str,
    bucket: "Bucket",
):
    "test isfile in ossfs"
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    file_foo = path + "foo"
    assert not ossfs.isfile("")
    assert not ossfs.isfile("/")
    assert not ossfs.isfile(path)

    assert not ossfs.isfile(file_foo)
    ossfs.invalidate_cache()
    bucket.put_object(bucket_relative_path(file_foo), "foo")
    assert ossfs.isfile(file_foo)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_isdir(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    "test isdir in ossfs"
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    file_foo = path + "foo"
    bucket.put_object(bucket_relative_path(file_foo), "foo")
    assert not ossfs.isdir(file_foo)
    assert ossfs.isdir(test_path)
    assert ossfs.isdir(path)
    assert ossfs.isdir(path.rstrip("/"))


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_rm(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file_foo = path + "foo"
    nest_file = path + "nested/nested2/file1"
    bucket.put_object(bucket_relative_path(file_foo), "foo")
    assert bucket.object_exists(bucket_relative_path(file_foo))
    ossfs.rm(file_foo)
    assert not bucket.object_exists(bucket_relative_path(file_foo))
    with pytest.raises(FileNotFoundError):
        ossfs.rm("nonexistent")
    bucket.put_object(bucket_relative_path(nest_file), "foo")
    ossfs.rm(path + "nested", recursive=True)
    assert not bucket.object_exists(bucket_relative_path(nest_file))


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_bulk_delete(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    nest_file1 = path + "nested/file1"
    nest_file2 = path + "nested/file2"
    bucket.put_object(bucket_relative_path(nest_file1), "foo")
    bucket.put_object(bucket_relative_path(nest_file2), "bar")
    filelist = ossfs.find(path + "nested/")
    assert set(filelist) == {nest_file1, nest_file2}
    ossfs.rm(filelist)
    assert not bucket.object_exists(bucket_relative_path(nest_file1))
    assert not bucket.object_exists(bucket_relative_path(nest_file2))


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ossfs_file_access(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], number_file: str
):
    assert ossfs.cat(number_file) == NUMBERS
    assert ossfs.head(number_file, 3) == NUMBERS[:3]
    assert ossfs.tail(number_file, 3) == NUMBERS[-3:]
    assert ossfs.tail(number_file, 10000) == NUMBERS
    assert ossfs.info(number_file)["Size"] == len(NUMBERS)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_du(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file1 = path + "file1"
    file2 = path + "dir/file2"
    file3 = path + "dir/file3"
    bucket.put_object(bucket_relative_path(file1), b"1234567890")
    bucket.put_object(bucket_relative_path(file2), b"12345")
    bucket.put_object(bucket_relative_path(file3), b"1234567890" * 2)

    d = ossfs.du(path, total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert d[file1] == 10
    assert d[file2] == 5
    assert d[file3] == 20
    assert ossfs.du(path, total=True) == 35


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ossfs_ls(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    filename = path + "nested/file1"
    bucket.put_object(bucket_relative_path(filename), "foo")
    assert filename not in ossfs.ls(path, detail=False)
    assert filename in ossfs.ls(path + "nested/", detail=False)
    assert filename in ossfs.ls(path + "nested", detail=False)
    L = ossfs.ls(path + "nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ossfs_big_ls(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    for num in range(120):
        bucket.put_object(bucket_relative_path(f"{path}{num}.part"), "foo")

    assert len(ossfs.ls(path, detail=False, connect_timeout=600)) == 120


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_ossfs_glob(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file1 = path + "nested/file.dat"
    file2 = path + "nested/filedat"
    bucket.put_object(bucket_relative_path(file1), "foo")
    bucket.put_object(bucket_relative_path(file2), "bar")
    assert file1 not in ossfs.glob(path + "")
    assert file1 not in ossfs.glob(path + "*")
    assert file1 not in ossfs.glob(path + "nested")
    assert file1 in ossfs.glob(path + "nested/*")
    assert file1 in ossfs.glob(path + "nested/file*")
    assert all(
        any(p.startswith(f + "/") or p == f for p in ossfs.find(path))
        for f in ossfs.glob(path + "nested/*")
    )
    out = ossfs.glob(path + "nested/*")
    assert {file1, file2} == set(out)

    # Make sure glob() deals with the dot character (.) correctly.
    assert file1 in ossfs.glob(path + "nested/file.*")
    assert file2 not in ossfs.glob(path + "nested/file.*")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_copy(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    number_file: str,
    test_path: str,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    new_file = path + "file"
    ossfs.copy(number_file, new_file)
    assert bucket.get_object(bucket_relative_path(new_file)).read() == NUMBERS


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_move(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    number_file: str,
    test_path: str,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    from_file = path + "from"
    to_file = path + "to"
    ossfs.copy(number_file, from_file)
    assert bucket.object_exists(bucket_relative_path(from_file))
    assert not bucket.object_exists(bucket_relative_path(to_file))
    ossfs.mv(from_file, to_file)
    assert bucket.get_object(bucket_relative_path(to_file)).read() == NUMBERS
    assert not bucket.object_exists(bucket_relative_path(from_file))


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_get_put(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    tmpdir,
    test_path: str,
    size: int,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"

    local_file = str(tmpdir.join("number"))
    data = os.urandom(size)
    open(local_file, "wb").write(data)

    remote_file = path + "file"
    ossfs.put(local_file, remote_file)
    assert bucket.object_exists(bucket_relative_path(remote_file))
    assert bucket.get_object(bucket_relative_path(remote_file)).read() == data

    get_file = str(tmpdir.join("get"))
    ossfs.get(remote_file, get_file)
    assert open(get_file, "rb").read() == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
@pytest.mark.parametrize("size", [2**10, 20 * 2**20])
def test_pipe_cat_big(
    ossfs: "OSSFileSystem", size: int, test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    bigfile = path + "bigfile"
    data = os.urandom(size)
    ossfs.pipe(bigfile, data)
    assert bucket.get_object(bucket_relative_path(bigfile)).read() == data


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_errors(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"

    with pytest.raises(FileNotFoundError):
        ossfs.open(path + "none", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.mv(path + "x", path + "y")

    with pytest.raises(ValueError):
        ossfs.open("x", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.open("xxxx", "rb")

    with pytest.raises(PermissionError):
        ossfs.rm("/non-exist-bucket")

    with pytest.raises(ValueError):
        with ossfs.open(path + "temp", "wb") as f:
            f.read()

    bucket.put_object(path.split("/", 2)[-1] + "temp", "foobar")
    with pytest.raises(ValueError):
        f = ossfs.open(path + "temp", "rb")
        f.close()
        f.read()


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_touch(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    # create
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}/"
    filename = path + "file"
    assert not bucket.object_exists(bucket_relative_path(filename))
    ossfs.touch(filename)
    assert bucket.object_exists(bucket_relative_path(filename))
    assert bucket.get_object(bucket_relative_path(filename)).read() == b""

    bucket.put_object(bucket_relative_path(filename), b"data")
    with pytest.raises(NotImplementedError):
        ossfs.touch(filename, truncate=False)
    assert bucket.get_object(bucket_relative_path(filename)).read() == b"data"


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_cat_missing(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file1 = path + "file0"
    file2 = path + "file1"
    ossfs.touch(file1)
    bucket.put_object(bucket_relative_path(file1), b"foo")
    with pytest.raises(FileNotFoundError):
        ossfs.cat([file1, file2], on_error="raise")
    out = ossfs.cat([file1, file2], on_error="omit")
    assert list(out) == [file1]
    out = ossfs.cat([file1, file2], on_error="return")
    assert file2 in out
    assert isinstance(out[file2], FileNotFoundError)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_get_directories(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    tmpdir,
    test_path: str,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    bucket.put_object(bucket_relative_path(path + "dir/dirkey/key0"), "")
    bucket.put_object(bucket_relative_path(path + "dir/dirkey/key1"), "")
    bucket.put_object(bucket_relative_path(path + "dir/dir/key"), "")
    d = str(tmpdir)
    ossfs.get(path + "dir/", d, recursive=True)
    assert {"dirkey", "dir"} == set(os.listdir(d))
    assert ["key"] == os.listdir(os.path.join(d, "dir"))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dirkey")))


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_modified(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_path: str,
    test_bucket_name: str,
    bucket: "Bucket",
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file_path = path + "file"
    notexist_path = path + "notexist"

    # Test file
    bucket.put_object(bucket_relative_path(file_path), "")
    modified = ossfs.modified(path=file_path)
    assert isinstance(modified, int)

    # Test file
    with pytest.raises(FileNotFoundError):
        modified = ossfs.modified(path=notexist_path)

    # Test directory
    with pytest.raises(NotImplementedError):
        modified = ossfs.modified(path=path)

    # Test bucket
    with pytest.raises(NotImplementedError):
        ossfs.modified(path=test_bucket_name)


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_find_file_info_with_selector(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    file_a = path + "test_file_a"
    file_b = path + "test_file_b"
    dir_a = path + "test_dir_a"
    file_c = path + "test_dir_a/test_file_c"

    bucket.put_object(bucket_relative_path(file_a), "")
    bucket.put_object(bucket_relative_path(file_b), "")
    bucket.put_object(bucket_relative_path(file_c), "")

    infos = ossfs.find(path, maxdepth=None, withdirs=True, detail=True)
    assert len(infos) == 4

    for info in infos.values():
        if info["name"].endswith(file_a):
            assert info["type"] == "file"
        elif info["name"].endswith(file_b):
            assert info["type"] == "file"
        elif info["name"].endswith(file_c):
            assert info["type"] == "file"
        elif info["name"].rstrip("/").endswith(dir_a):
            assert info["type"] == "directory"
        else:
            raise ValueError(f"unexpected path {info['name']}")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_exists(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/{ossfs.__class__.__name__}"
    bucket.put_object(bucket_relative_path(path + "very/similar/prefix1"), "")
    bucket.put_object(bucket_relative_path(path + "very/similar/prefix2"), "")
    bucket.put_object(bucket_relative_path(path + "very/similar/prefix3/something"), "")

    assert not ossfs.exists(path + "very/similar/prefix")
    assert not ossfs.exists(path + "very/similar/prefi")
    assert not ossfs.exists(path + "very/similar/pref")

    assert ossfs.exists(path + "very/similar/")
    assert ossfs.exists(path + "very/similar/prefix1")
    assert ossfs.exists(path + "very/similar/prefix2")
    assert ossfs.exists(path + "very/similar/prefix3")
    assert ossfs.exists(path + "very/similar/prefix3/")
    assert ossfs.exists(path + "very/similar/prefix3/something")

    assert not ossfs.exists(path + "very/similar/prefix3/some")

    bucket.put_object(bucket_relative_path(path + "starting/very/similar/prefix"), "")

    assert not ossfs.exists(path + "starting/very/similar/prefix1")
    assert not ossfs.exists(path + "starting/very/similar/prefix3")
    assert not ossfs.exists(path + "starting/very/similar/prefix3/")
    assert not ossfs.exists(path + "starting/very/similar/prefix3/something")

    assert ossfs.exists(path + "starting/very/similar/prefix")
    assert not ossfs.exists(path + "starting/very/similar/prefix/")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_leading_forward_slash(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    bucket.put_object(bucket_relative_path(path + "some/file"), "")
    assert ossfs.ls(path + "some/")
    path = path.lstrip("/")
    assert ossfs.exists(path + "some/file")
    assert ossfs.exists("/" + path + "some/file")


@pytest.mark.parametrize("ossfs", ["sync", "async"], indirect=True)
def test_find_with_prefix(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    if not ossfs.exists(path):
        for cursor in range(100):
            bucket.put_object(
                bucket_relative_path(path + f"prefixes/test_{cursor}"), ""
            )
        bucket.put_object(bucket_relative_path(path + "prefixes2"), "")

    assert len(ossfs.find(path + "prefixes")) == 100
    assert len(ossfs.find(path, prefix="prefixes")) == 101

    assert len(ossfs.find(path + "prefixes/test_")) == 0
    assert len(ossfs.find(path + "prefixes", prefix="test_")) == 100
    assert len(ossfs.find(path + "prefixes/", prefix="test_")) == 100

    test_1s = ossfs.find(path + "prefixes/test_1")
    assert len(test_1s) == 1
    assert test_1s[0] == path + "prefixes/test_1"

    test_1s = ossfs.find(path + "prefixes/", prefix="test_1")
    assert len(test_1s) == 11
    assert test_1s == [path + "prefixes/test_1"] + [
        path + f"prefixes/test_{cursor}" for cursor in range(10, 20)
    ]


WRITE_BLOCK_SIZE = 2**13  # 8KB blocks
READ_BLOCK_SIZE = 2**14  # 16KB blocks


@pytest.mark.parametrize("ossfs", ["sync"], indirect=True)
def test_get_put_file(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], tmpdir, test_path: str
):
    function_name = inspect.stack()[0][0].f_code.co_name
    path = f"{test_path}/{function_name}/"
    src_file = str(tmpdir / "source")
    src2_file = str(tmpdir / "source_2")
    dest_file = path + "get_put_file/dest"

    data = b"test" * 2**20

    # pylint: disable=missing-class-docstring
    class EventLogger(fsspec.Callback):
        def __init__(self):
            self.events = []
            super().__init__()

        def set_size(self, size):
            self.events.append(("set_size", size))

        def absolute_update(self, value):
            self.events.append(("absolute_update", value))

    with open(src_file, "wb") as f_write:
        f_write.write(data)

    event_logger = EventLogger()
    ossfs.put_file(src_file, dest_file, callback=event_logger)
    assert ossfs.exists(dest_file)

    assert event_logger.events[0] == ("set_size", len(data))
    assert len(event_logger.events[1:]) == len(data) // WRITE_BLOCK_SIZE

    event_logger = EventLogger()
    ossfs.get_file(dest_file, src2_file, callback=event_logger)
    with open(src2_file, "rb") as f_read:
        assert f_read.read() == data

    assert event_logger.events[0] == ("set_size", len(data))
    assert len(event_logger.events[1:]) == len(data) // READ_BLOCK_SIZE
