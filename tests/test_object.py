"""
Test all oss object related methods
"""
# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
import os
import time

import pytest

test_bucket_name = "dvc-temp"


def test_info(ossfs):
    "test file info"
    # test file info
    test_info_a = test_bucket_name + "/test_info/a"
    test_info_b = test_bucket_name + "/test_info/b"
    ossfs.rm([test_info_a, test_info_b])
    ossfs.touch(test_info_a)
    ossfs.touch(test_info_b)
    info = ossfs.info(test_info_a)
    linfo = ossfs.ls(test_info_a, detail=True)[0]
    print(info)
    print(linfo)
    assert abs(info.pop("LastModified") - linfo.pop("LastModified")) <= 1
    assert info == linfo

    # test not exist dir
    new_path = test_bucket_name + "/test_info/foo"
    file_in_new_path = new_path + "/bar"
    if ossfs.exists(file_in_new_path):
        ossfs.rm(file_in_new_path)
    with pytest.raises(FileNotFoundError):
        ossfs.info(new_path)

    # add a new file then we can info this new dir.
    ossfs.touch(file_in_new_path)
    ossfs.info(new_path)


def test_checksum(ossfs):
    bucket = test_bucket_name
    d = "checksum"
    prefix = d + "/e"
    o1 = prefix + "1"
    path1 = bucket + "/" + o1

    # change one file, using cache
    with ossfs.open(path1, "w") as f_w:
        f_w.write("foo")
    checksum = ossfs.checksum(path1)

    # Test changed contents
    with ossfs.open(path1, "w") as f_w:
        f_w.write("bar")
    assert checksum != ossfs.checksum(path1)
    # Test for same contents

    # sleep 1 second to make sure the last modificed changes
    time.sleep(1)
    with ossfs.open(path1, "w") as f_w:
        f_w.write("foo")
    assert checksum != ossfs.checksum(path1)

    # Test for nonexistent file
    ossfs.touch(path1)
    ossfs.ls(path1)  # force caching
    ossfs.rm(path1)
    with pytest.raises(FileNotFoundError):
        ossfs.checksum(path1)


def test_ls_object(ossfs):
    with pytest.raises(FileNotFoundError):
        ossfs.ls(test_bucket_name + "/nonexistent")
    fn = test_bucket_name + "/test/accounts.1.json"
    ossfs.touch(fn)
    assert fn in ossfs.ls(test_bucket_name + "/test/", detail=False)


def test_ls_touch(ossfs):
    file_a = test_bucket_name + "/test_ls_touch/a"
    file_b = test_bucket_name + "/test_ls_touch/b"
    if ossfs.exists(test_bucket_name + "/test_ls_touch/"):
        ossfs.rm(test_bucket_name + "/test_ls_touch/", recursive=True)
    assert not ossfs.exists(test_bucket_name + "/test_ls_touch/")
    ossfs.touch(file_a)
    ossfs.touch(file_b)
    L = ossfs.ls(test_bucket_name + "/test_ls_touch/", True)
    assert {d["Key"] for d in L} == {file_a, file_b}
    L = ossfs.ls(test_bucket_name + "/test_ls_touch/", False)
    assert set(L) == {file_a, file_b}


def test_isfile(ossfs):
    "test isfile in ossfs"
    file_a = test_bucket_name + "/test_is_file/a"
    assert not ossfs.isfile("")
    assert not ossfs.isfile("/")
    assert not ossfs.isfile(test_bucket_name)

    ossfs.rm_file(file_a)
    assert not ossfs.isfile(file_a)
    ossfs.touch(file_a)
    assert ossfs.isfile(file_a)


def test_isdir(ossfs):
    "test isdir in ossfs"
    file_a = test_bucket_name + "/test_is_dir/a"
    assert ossfs.isdir("")
    assert ossfs.isdir("/")
    assert ossfs.isdir(test_bucket_name)

    ossfs.touch(file_a)
    assert not ossfs.isdir(file_a)
    assert ossfs.isdir(test_bucket_name + "/test_is_dir/")
    assert ossfs.isdir(test_bucket_name + "/test_is_dir")


def test_rm(ossfs):
    file_a = test_bucket_name + "/test_rm/a"
    nest_file = test_bucket_name + "/test_rm/nested/nested2/file1"
    ossfs.touch(file_a)
    assert ossfs.exists(file_a)
    ossfs.rm(file_a)
    assert not ossfs.exists(file_a)
    with pytest.raises(FileNotFoundError):
        ossfs.rm("nonexistent")
    ossfs.touch(nest_file)
    ossfs.rm(test_bucket_name + "/test_rm/nested", recursive=True)
    assert not ossfs.exists(nest_file)


def test_bulk_delete(ossfs):
    nest_file1 = test_bucket_name + "/test_bulk_delete/nested/nested2/file1"
    nest_file2 = test_bucket_name + "/test_bulk_delete/nested/nested2/file2"
    ossfs.touch(nest_file1)
    ossfs.touch(nest_file2)
    filelist = ossfs.find(test_bucket_name + "/test_bulk_delete")
    ossfs.rm(filelist)
    assert not ossfs.exists(
        test_bucket_name + "/test_bulk_delete/nested/nested2/file1"
    )


def test_ossfs_file_access(ossfs):
    fn = test_bucket_name + "/number"
    data = b"1234567890\n"
    assert ossfs.cat(fn) == data
    assert ossfs.head(fn, 3) == data[:3]
    assert ossfs.tail(fn, 3) == data[-3:]
    assert ossfs.tail(fn, 10000) == data
    assert ossfs.info(fn)["Size"] == len(data)


def test_du(ossfs):
    path = test_bucket_name + "/test_du"
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
    print(d)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert d[file1] == 10
    assert d[file2] == 5
    assert d[file3] == 20

    assert ossfs.du(path, total=True) == 35


def test_ossfs_ls(ossfs):
    path = test_bucket_name + "/test_ossfs_ls"
    fn = path + "/nested/file1"
    ossfs.touch(fn)
    assert fn not in ossfs.ls(path, detail=False)
    assert fn in ossfs.ls(path + "/nested/", detail=False)
    assert fn in ossfs.ls(path + "/nested", detail=False)
    L = ossfs.ls(path + "/nested", detail=True)
    assert all(isinstance(item, dict) for item in L)


def test_ossfs_big_ls(ossfs):
    path = test_bucket_name + "/big_ls"
    for x in range(1200):
        ossfs.touch(path + "/thousand/%i.part" % x)
    assert len(ossfs.find(path)) == 1200
    ossfs.rm(path + "/thousand/", recursive=True)
    assert len(ossfs.find(path + "/thousand/")) == 0


def test_ossfs_glob(ossfs):
    path = test_bucket_name + "/test_ossfs_glob"
    fn = path + "/nested/file.dat"
    fn2 = path + "/nested/filedat"
    ossfs.rm(path, recursive=True)
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


def test_copy(ossfs):
    fn = test_bucket_name + "/number"
    if ossfs.exists(fn + "2"):
        ossfs.rm(fn + "2")
    ossfs.copy(fn, fn + "2")
    assert ossfs.cat(fn) == ossfs.cat(fn + "2")


def test_move(ossfs):
    fn = test_bucket_name + "/number"
    if ossfs.exists(fn + "2"):
        ossfs.rm(fn + "2")
        ossfs.copy(fn, fn + "2")
    if ossfs.exists(fn + "3"):
        ossfs.rm(fn + "3")
    assert ossfs.exists(fn + "2")
    assert not ossfs.exists(fn + "3")
    data = ossfs.cat(fn + "2")
    ossfs.mv(fn + "2", fn + "3")
    assert ossfs.cat(fn + "3") == data
    assert not ossfs.exists(fn + "2")


def test_get_put(ossfs, tmpdir):
    test_file = str(tmpdir.join("number"))

    assert ossfs.exists(test_bucket_name + "/number")
    ossfs.get(test_bucket_name + "/number", test_file)
    data = b"1234567890\n"
    assert open(test_file, "rb").read() == data
    new_put_file = test_bucket_name + "/number_temp"
    if ossfs.exists(new_put_file):
        ossfs.rm(new_put_file)
    ossfs.put(test_file, new_put_file)
    assert ossfs.du(test_bucket_name, total=False)[new_put_file] == len(data)
    assert ossfs.cat(test_bucket_name + "/number_temp") == data
    with pytest.raises(FileExistsError):
        ossfs.put(test_file, new_put_file)


def test_get_put_big(ossfs, tmpdir):
    bigfile = test_bucket_name + "/bigfile"
    test_file = str(tmpdir.join("test"))
    data = b"1234567890A" * 2 ** 20
    open(test_file, "wb").write(data)

    if ossfs.exists(bigfile):
        ossfs.rm(bigfile)
    ossfs.put(test_file, bigfile)
    test_file = str(tmpdir.join("test2"))
    ossfs.get(bigfile, test_file)
    assert open(test_file, "rb").read() == data


@pytest.mark.parametrize("size", [2 ** 10, 2 ** 20, 10 * 2 ** 20])
def test_pipe_cat_big(ossfs, size):
    data = b"1234567890A" * size
    ossfs.pipe(test_bucket_name + "/bigfile", data)
    assert ossfs.cat(test_bucket_name + "/bigfile") == data


def test_errors(ossfs):
    with pytest.raises(FileNotFoundError):
        ossfs.open(test_bucket_name + "/tmp/test/shfoshf", "rb")

    # This is fine, no need for interleaving directories on S3
    # with pytest.raises((IOError, OSError)):
    #    ossfs.touch('tmp/test/shfoshf/x')

    # Deleting nonexistent or zero paths is allowed for now
    # with pytest.raises(FileNotFoundError):
    #    ossfs.rm(test_bucket_name + '/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        ossfs.mv(
            test_bucket_name + "/tmp/test/shfoshf/x", "tmp/test/shfoshf/y"
        )

    with pytest.raises(ValueError):
        ossfs.open("x", "rb")

    with pytest.raises(ValueError):
        ossfs.rm("unknown")

    with pytest.raises(ValueError):
        with ossfs.open(test_bucket_name + "/temp", "wb") as f:
            f.read()

    with pytest.raises(ValueError):
        f = ossfs.open(test_bucket_name + "/temp", "rb")
        f.close()
        f.read()


def test_touch(ossfs):
    # create
    fn = test_bucket_name + "/touched"
    ossfs.rm(fn)
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


def test_cat_missing(ossfs):
    fn0 = test_bucket_name + "/file0"
    fn1 = test_bucket_name + "/file1"
    ossfs.touch(fn0)
    with pytest.raises(FileNotFoundError):
        ossfs.cat([fn0, fn1], on_error="raise")
    out = ossfs.cat([fn0, fn1], on_error="omit")
    assert list(out) == [fn0]
    out = ossfs.cat([fn0, fn1], on_error="return")
    assert fn1 in out
    assert isinstance(out[fn1], FileNotFoundError)


def test_get_directories(ossfs, tmpdir):
    path = test_bucket_name + "/test_get_directories"
    ossfs.rm(path + "/dir", recursive=True)
    ossfs.touch(path + "/dir/dirkey/key0")
    ossfs.touch(path + "/dir/dirkey/key1")
    ossfs.touch(path + "/dir/dir/key")
    d = str(tmpdir)
    ossfs.get(path + "/dir", d, recursive=True)
    assert {"dirkey", "dir"} == set(os.listdir(d))
    assert ["key"] == os.listdir(os.path.join(d, "dir"))
    assert {"key0", "key1"} == set(os.listdir(os.path.join(d, "dirkey")))


def test_lsdir(ossfs):
    ossfs.find(test_bucket_name)

    d = test_bucket_name + "/test"
    assert d in ossfs.ls(test_bucket_name, detail=False)


def test_modified(ossfs):
    dir_path = test_bucket_name + "/modified"
    file_path = dir_path + "/file"
    notexist_path = dir_path + "/notexist"

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
        ossfs.modified(path=test_bucket_name)


def test_find_no_side_effect(ossfs):
    infos1 = ossfs.find(
        test_bucket_name, maxdepth=1, withdirs=True, detail=True
    )
    ossfs.find(test_bucket_name, maxdepth=None, withdirs=True, detail=True)
    infoossfs = ossfs.find(
        test_bucket_name, maxdepth=1, withdirs=True, detail=True
    )
    assert infos1.keys() == infoossfs.keys()


def test_get_file_info_with_selector(ossfs):

    base_dir = test_bucket_name + "/selector-dir"
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
            raise ValueError("unexpected path {}".format(info["name"]))
    ossfs.rm(base_dir, recursive=True)


def test_same_name_but_no_exact(ossfs):
    ossfs.rm(test_bucket_name + "/very/similiar", recursive=True)
    ossfs.touch(test_bucket_name + "/very/similiar/prefix1")
    ossfs.touch(test_bucket_name + "/very/similiar/prefix2")
    ossfs.touch(test_bucket_name + "/very/similiar/prefix3/something")
    assert not ossfs.exists(test_bucket_name + "/very/similiar/prefix")
    assert not ossfs.exists(test_bucket_name + "/very/similiar/prefi")
    assert not ossfs.exists(test_bucket_name + "/very/similiar/pref")

    assert ossfs.exists(test_bucket_name + "/very/similiar/")
    assert ossfs.exists(test_bucket_name + "/very/similiar/prefix1")
    assert ossfs.exists(test_bucket_name + "/very/similiar/prefix2")
    assert ossfs.exists(test_bucket_name + "/very/similiar/prefix3")
    assert ossfs.exists(test_bucket_name + "/very/similiar/prefix3/")
    assert ossfs.exists(test_bucket_name + "/very/similiar/prefix3/something")

    assert not ossfs.exists(test_bucket_name + "/very/similiar/prefix3/some")

    ossfs.touch(test_bucket_name + "/starting/very/similiar/prefix")

    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix1"
    )
    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix2"
    )
    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix3"
    )
    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix3/"
    )
    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix3/something"
    )

    assert ossfs.exists(test_bucket_name + "/starting/very/similiar/prefix")
    assert not ossfs.exists(
        test_bucket_name + "/starting/very/similiar/prefix/"
    )


def test_leading_forward_slash(ossfs):
    ossfs.touch(test_bucket_name + "/some/file")
    assert ossfs.ls(test_bucket_name + "/some/")
    assert ossfs.exists(test_bucket_name + "/some/file")
    assert ossfs.exists("/" + test_bucket_name + "/some/file")


def test_invalid_cache():
    pass
