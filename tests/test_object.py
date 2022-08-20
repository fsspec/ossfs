"""
Test all oss object related methods
"""
# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
# pylint:disable=consider-using-with
import os
import time

import fsspec
import pytest


def test_info(ossfs, test_path):
    "test file info"
    # test file info
    test_info_a = test_path + "/test_info/a"
    test_info_b = test_path + "/test_info/b"
    ossfs.touch(test_info_a)
    ossfs.touch(test_info_b)
    info = ossfs.info(test_info_a)
    linfo = ossfs.ls(test_info_a, detail=True)[0]
    assert abs(info.pop("LastModified") - linfo.pop("LastModified")) <= 1
    assert info == linfo

    # test not exist dir
    new_path = test_path + "/test_info/foo"
    file_in_new_path = new_path + "/bar"
    with pytest.raises(FileNotFoundError):
        ossfs.info(new_path)

    # add a new file then we can info this new dir.
    ossfs.touch(file_in_new_path)
    ossfs.info(new_path)


def test_checksum(ossfs, test_path):
    file = test_path + "/test_check_sum/file"

    # change one file, using cache
    with ossfs.open(file, "w") as f_w:
        f_w.write("foo")
    checksum = ossfs.checksum(file)

    # Test changed contents
    with ossfs.open(file, "w") as f_w:
        f_w.write("bar")
    assert checksum != ossfs.checksum(file)
    # Test for same contents

    # sleep 1 second to make sure the last modificed changes
    time.sleep(1)
    with ossfs.open(file, "w") as f_w:
        f_w.write("foo")
    assert checksum != ossfs.checksum(file)

    # Test for nonexistent file
    ossfs.rm(file)
    with pytest.raises(FileNotFoundError):
        ossfs.checksum(file)


def test_ls_object(ossfs, test_path):
    path = test_path + "/test_ls_object/"
    assert ossfs.ls(test_path + "/nonexistent") == []
    fn = path + "accounts.1.json"
    ossfs.touch(fn)
    assert fn in ossfs.ls(path, detail=False)


def test_ls_touch(ossfs, test_path):
    path = test_path + "/test_ls_touch"
    file_a = path + "/a"
    file_b = path + "/b"
    assert not ossfs.exists(test_path + "/test_ls_touch/")
    ossfs.touch(file_a)
    ossfs.touch(file_b)
    L = ossfs.ls(test_path + "/test_ls_touch/", True)
    assert {d["Key"] for d in L} == {file_a, file_b}
    L = ossfs.ls(test_path + "/test_ls_touch/", False)
    assert set(L) == {file_a, file_b}


def test_isfile(ossfs, test_path):
    "test isfile in ossfs"
    file_a = test_path + "/test_isfile/a"
    assert not ossfs.isfile("")
    assert not ossfs.isfile("/")
    assert not ossfs.isfile(test_path)

    assert not ossfs.isfile(file_a)
    ossfs.touch(file_a)
    assert ossfs.isfile(file_a)


def test_isdir(ossfs, test_path):
    "test isdir in ossfs"
    file_a = test_path + "/test_isdir/a"
    ossfs.touch(file_a)
    assert ossfs.isdir("")
    assert ossfs.isdir("/")
    assert not ossfs.isdir(file_a)
    assert ossfs.isdir(test_path)
    assert ossfs.isdir(test_path + "/test_isdir/")
    assert ossfs.isdir(test_path + "/test_isdir")


def test_rm(ossfs, test_path):
    file_a = test_path + "/test_rm/a"
    nest_file = test_path + "/test_rm/nested/nested2/file1"
    ossfs.touch(file_a)
    assert ossfs.exists(file_a)
    ossfs.rm(file_a)
    assert not ossfs.exists(file_a)
    with pytest.raises(FileNotFoundError):
        ossfs.rm("nonexistent")
    ossfs.touch(nest_file)
    ossfs.rm(test_path + "/test_rm/nested", recursive=True)
    assert not ossfs.exists(nest_file)


def test_bulk_delete(ossfs, test_path):
    nest_file1 = test_path + "/test_bulk_delete/nested/nested2/file1"
    nest_file2 = test_path + "/test_bulk_delete/nested/nested2/file2"
    ossfs.touch(nest_file1)
    ossfs.touch(nest_file2)
    filelist = ossfs.find(test_path + "/test_bulk_delete")
    ossfs.rm(filelist)
    assert not ossfs.exists(
        test_path + "/test_bulk_delete/nested/nested2/file1"
    )


def test_ossfs_file_access(ossfs, number_file):
    data = b"1234567890\n"
    assert ossfs.cat(number_file) == data
    assert ossfs.head(number_file, 3) == data[:3]
    assert ossfs.tail(number_file, 3) == data[-3:]
    assert ossfs.tail(number_file, 10000) == data
    assert ossfs.info(number_file)["Size"] == len(data)


def test_du(ossfs, test_path):
    path = test_path + "/test_du"
    file1 = path + "/file1"
    file2 = path + "/dir/file2"
    file3 = path + "/dir/file3"
    with ossfs.open(file1, "wb") as f:
        f.write(b"1234567890")
    with ossfs.open(file2, "wb") as f:
        f.write(b"12345")
    with ossfs.open(file3, "wb") as f:
        f.write(b"1234567890" * 2)
    d = ossfs.du(path, total=False)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert d[file1] == 10
    assert d[file2] == 5
    assert d[file3] == 20

    assert ossfs.du(path, total=True) == 35


def test_ossfs_ls(ossfs, test_path):
    path = test_path + "/test_ossfs_ls"
    fn = path + "/nested/file1"
    ossfs.touch(fn)
    assert fn not in ossfs.ls(path, detail=False)
    assert fn in ossfs.ls(path + "/nested/", detail=False)
    assert fn in ossfs.ls(path + "/nested", detail=False)
    L = ossfs.ls(path + "/nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_ossfs_big_ls(ossfs, test_path):
    path = test_path + "/test_ossfs_big_ls"
    if not ossfs.exists(path):
        for x in range(1200):
            ossfs.touch(path + f"/{x}.part")
    files = ossfs.find(path, connect_timeout=600)
    for x in range(1200):
        file = path + f"/{x}.part"
        if file not in files:
            ossfs.touch(path + f"/{x}.part")

    assert len(ossfs.find(path, connect_timeout=600)) == 1200


def test_ossfs_glob(ossfs, test_path):
    path = test_path + "/test_ossfs_glob"
    fn = path + "/nested/file.dat"
    fn2 = path + "/nested/filedat"
    ossfs.touch(fn)
    ossfs.touch(fn2)
    assert fn not in ossfs.glob(path + "/")
    assert fn not in ossfs.glob(path + "/*")
    assert fn not in ossfs.glob(path + "/nested")
    assert fn in ossfs.glob(path + "/nested/*")
    assert fn in ossfs.glob(path + "/nested/file*")
    assert all(
        any(p.startswith(f + "/") or p == f for p in ossfs.find(path))
        for f in ossfs.glob(path + "/nested/*")
    )
    out = ossfs.glob(path + "/nested/*")
    assert {fn, fn2} == set(out)

    # Make sure glob() deals with the dot character (.) correctly.
    assert fn in ossfs.glob(path + "/nested/file.*")
    assert fn2 not in ossfs.glob(path + "/nested/file.*")


def test_copy(ossfs, number_file, test_path):
    new_file = test_path + "/test_copy/file"
    ossfs.copy(number_file, new_file)
    assert ossfs.cat(number_file) == ossfs.cat(new_file)


def test_move(ossfs, number_file, test_path):
    from_file = test_path + "/test_move/from"
    to_file = test_path + "/test_move/to"
    ossfs.copy(number_file, from_file)
    assert ossfs.exists(from_file)
    assert not ossfs.exists(to_file)
    data = ossfs.cat(from_file)
    ossfs.mv(from_file, to_file)
    assert ossfs.cat(to_file) == data
    assert not ossfs.exists(from_file)


@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_get_put(ossfs, tmpdir, test_path, size):
    local_file = str(tmpdir.join("number"))
    data = os.urandom(size)
    open(local_file, "wb").write(data)

    remote_file = test_path + "/test_get_put/file"
    ossfs.put(local_file, remote_file)
    assert ossfs.exists(remote_file)
    assert ossfs.cat(remote_file) == data

    get_file = str(tmpdir.join("get"))
    ossfs.get(remote_file, get_file)
    assert open(get_file, "rb").read() == data


@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_pipe_cat_big(ossfs, size, test_path):
    bigfile = test_path + "/test_get_put_big/bigfile"
    data = b"1234567890A" * size
    ossfs.pipe(bigfile, data)
    assert ossfs.cat(bigfile) == data


def test_errors(ossfs, test_path):
    with pytest.raises(FileNotFoundError):
        ossfs.open(test_path + "/test_errors/none", "rb")

    with pytest.raises(FileNotFoundError):
        ossfs.mv(test_path + "/tmp/test/shfoshf/x", "tmp/test/shfoshf/y")

    with pytest.raises(ValueError):
        ossfs.open("x", "rb")

    with pytest.raises(ValueError):
        ossfs.rm("/non_exist_bucket")

    with pytest.raises(ValueError):
        with ossfs.open(test_path + "/temp", "wb") as f:
            f.read()

    with pytest.raises(ValueError):
        f = ossfs.open(test_path + "/temp", "rb")
        f.close()
        f.read()


def test_touch(ossfs, test_path):
    # create
    fn = test_path + "/test_touch/file"
    assert not ossfs.exists(fn)
    ossfs.touch(fn)
    assert ossfs.exists(fn)
    assert ossfs.size(fn) == 0

    # truncates
    with ossfs.open(fn, "wb") as f:
        f.write(b"data")
    assert ossfs.size(fn) == 4

    # exists error
    with ossfs.open(fn, "wb") as f:
        f.write(b"data")
    assert ossfs.size(fn) == 4
    ossfs.touch(fn, truncate=False)
    assert ossfs.size(fn) == 4


def test_cat_missing(ossfs, test_path):
    fn0 = test_path + "/test_cat_missing/file0"
    fn1 = test_path + "/test_cat_missing/file1"
    ossfs.touch(fn0)
    with pytest.raises(FileNotFoundError):
        ossfs.cat([fn0, fn1], on_error="raise")
    out = ossfs.cat([fn0, fn1], on_error="omit")
    assert list(out) == [fn0]
    out = ossfs.cat([fn0, fn1], on_error="return")
    assert fn1 in out
    assert isinstance(out[fn1], FileNotFoundError)


def test_get_directories(ossfs, tmpdir, test_path):
    path = test_path + "/test_get_directories"
    ossfs.touch(path + "/dir/dirkey/key0")
    ossfs.touch(path + "/dir/dirkey/key1")
    ossfs.touch(path + "/dir/dir/key")
    d = str(tmpdir)
    ossfs.get(path + "/dir", d, recursive=True)
    assert {"dirkey", "dir"} == set(os.listdir(d))
    assert ["key"] == os.listdir(os.path.join(d, "dir"))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dirkey")))


def test_lsdir(ossfs, test_path):
    path = test_path + "/test_lsdir/"
    file = path + "file"
    ossfs.touch(file)
    assert file in ossfs.ls(path, detail=False)


def test_modified(ossfs, test_path):
    dir_path = test_path + "/test_modified/"
    file_path = dir_path + "file"
    notexist_path = dir_path + "notexist"

    # Test file
    ossfs.touch(file_path)
    modified = ossfs.modified(path=file_path)
    assert isinstance(modified, int)

    # Test file
    with pytest.raises(FileNotFoundError):
        modified = ossfs.modified(path=notexist_path)

    # Test directory
    with pytest.raises(NotImplementedError):
        modified = ossfs.modified(path=dir_path)

    # Test bucket
    with pytest.raises(NotImplementedError):
        ossfs.modified(path=test_path)


def test_get_file_info_with_selector(ossfs, test_path):

    base_dir = test_path + "/test_get_file_info_with_selector"
    file_a = base_dir + "/test_file_a"
    file_b = base_dir + "/test_file_b"
    dir_a = base_dir + "/test_dir_a"
    file_c = base_dir + "/test_dir_a/test_file_c"

    ossfs.touch(file_a)
    ossfs.touch(file_b)
    ossfs.touch(file_c)

    infos = ossfs.find(base_dir, maxdepth=None, withdirs=True, detail=True)
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


def test_same_name_but_no_exact(ossfs, test_path):
    path = test_path + "/test_same_name_but_no_exact"
    ossfs.touch(path + "/very/similiar/prefix1")
    ossfs.touch(path + "/very/similiar/prefix2")
    ossfs.touch(path + "/very/similiar/prefix3/something")
    assert not ossfs.exists(path + "/very/similiar/prefix")
    assert not ossfs.exists(path + "/very/similiar/prefi")
    assert not ossfs.exists(path + "/very/similiar/pref")

    assert ossfs.exists(path + "/very/similiar/")
    assert ossfs.exists(path + "/very/similiar/prefix1")
    assert ossfs.exists(path + "/very/similiar/prefix2")
    assert ossfs.exists(path + "/very/similiar/prefix3")
    assert ossfs.exists(path + "/very/similiar/prefix3/")
    assert ossfs.exists(path + "/very/similiar/prefix3/something")

    assert not ossfs.exists(path + "/very/similiar/prefix3/some")

    ossfs.touch(path + "/starting/very/similiar/prefix")

    assert not ossfs.exists(path + "/starting/very/similiar/prefix1")
    assert not ossfs.exists(path + "/starting/very/similiar/prefix3")
    assert not ossfs.exists(path + "/starting/very/similiar/prefix3/")
    assert not ossfs.exists(path + "/starting/very/similiar/prefix3/something")

    assert ossfs.exists(path + "/starting/very/similiar/prefix")
    assert not ossfs.exists(path + "/starting/very/similiar/prefix/")


def test_leading_forward_slash(ossfs, test_path):
    path = test_path + "/test_leading_forward_slash"
    ossfs.touch(path + "/some/file")
    assert ossfs.ls(path + "/some/")
    path = path.lstrip("/")
    assert ossfs.exists(path + "/some/file")
    assert ossfs.exists("/" + path + "/some/file")


def test_find_with_prefix(ossfs, test_path):
    path = test_path + "/test_find_with_prefix"
    if not ossfs.exists(path):
        for cursor in range(100):
            ossfs.touch(path + f"/prefixes/test_{cursor}")
        ossfs.touch(path + "/prefixes2")

    assert len(ossfs.find(path + "/prefixes")) == 100
    assert len(ossfs.find(path, prefix="prefixes")) == 101

    assert len(ossfs.find(path + "/prefixes/test_")) == 0
    assert len(ossfs.find(path + "/prefixes", prefix="test_")) == 100
    assert len(ossfs.find(path + "/prefixes/", prefix="test_")) == 100

    test_1s = ossfs.find(path + "/prefixes/test_1")
    assert len(test_1s) == 1
    assert test_1s[0] == path + "/prefixes/test_1"

    test_1s = ossfs.find(path + "/prefixes/", prefix="test_1")
    assert len(test_1s) == 11
    assert test_1s == [path + "/prefixes/test_1"] + [
        path + f"/prefixes/test_{cursor}" for cursor in range(10, 20)
    ]


WRITE_BLOCK_SIZE = 2**13  # 8KB blocks
READ_BLOCK_SIZE = 2**14  # 16KB blocks


def test_get_put_file(ossfs, tmpdir, test_path):
    src_file = str(tmpdir / "source")
    src2_file = str(tmpdir / "source_2")
    dest_file = test_path + "/get_put_file/dest"

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

    with open(src_file, "wb") as stream:
        stream.write(data)

    event_logger = EventLogger()
    ossfs.put_file(src_file, dest_file, callback=event_logger)
    assert ossfs.exists(dest_file)

    assert event_logger.events[0] == ("set_size", len(data))
    assert len(event_logger.events[1:]) == len(data) // WRITE_BLOCK_SIZE

    event_logger = EventLogger()
    ossfs.get_file(dest_file, src2_file, callback=event_logger)
    with open(src2_file, "rb") as stream:
        assert stream.read() == data

    assert event_logger.events[0] == ("set_size", len(data))
    assert len(event_logger.events[1:]) == len(data) // READ_BLOCK_SIZE
