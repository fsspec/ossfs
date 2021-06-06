"""
Test all OSSFile related methods
"""
# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
import io
from array import array

import pytest

test_bucket_name = "dvc-temp"
files = {
    "LICENSE": (
        b"                                 Apache License\n"
        b"                           Version 2.0, January 2004\n"
        b"                        http://www.apache.org/licenses/\n"
    ),
    "number": (b"1234567890\n"),
}

glob_files = {"file.dat": b"", "filexdat": b""}
test_file_a = test_bucket_name + "/tmp/test/a"
test_file_b = test_bucket_name + "/tmp/test/b"
test_file_c = test_bucket_name + "/tmp/test/c"


def test_seek(ossfs):
    with ossfs.open(test_file_a, "wb") as f:
        f.write(b"123")

    with ossfs.open(test_file_a) as f:
        f.seek(1000)
        with pytest.raises(ValueError):
            f.seek(-1)
        with pytest.raises(ValueError):
            f.seek(-5, 2)
        with pytest.raises(ValueError):
            f.seek(0, 10)
        f.seek(0)
        assert f.read(1) == b"1"
        f.seek(0)
        assert f.read(1) == b"1"
        f.seek(3)
        assert f.read(1) == b""
        f.seek(-1, 2)
        assert f.read(1) == b"3"
        f.seek(-1, 1)
        f.seek(-1, 1)
        assert f.read(1) == b"2"
        for i in range(4):
            assert f.seek(i) == i


def test_bad_open(ossfs):
    with pytest.raises(ValueError):
        ossfs.open("")


def test_read_small(ossfs):
    fn = test_bucket_name + "/number"
    with ossfs.open(fn, "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(2)
            if data == b"":
                break
            out.append(data)
        assert ossfs.cat(fn) == b"".join(out)


def test_read_ossfs_block(ossfs):
    data = files["LICENSE"]
    lines = io.BytesIO(data).readlines()
    path = test_bucket_name + "/LICENSE"
    assert ossfs.read_block(path, 0, 10, b"\n") == lines[0]
    assert ossfs.read_block(path, 40, 10, b"\n") == lines[1]
    assert ossfs.read_block(path, 0, 80, b"\n") == lines[0] + lines[1]
    assert ossfs.read_block(path, 0, 120, b"\n") == data

    data = files["number"]
    lines = io.BytesIO(data).readlines()
    path = test_bucket_name + "/number"
    assert len(ossfs.read_block(path, 0, 5)) == 5
    assert len(ossfs.read_block(path, 4, 150)) == len(data) - 4
    assert ossfs.read_block(path, 20, 25) == b""

    assert ossfs.read_block(path, 5, None) == ossfs.read_block(path, 5, 25)


def test_write_small(ossfs):
    with ossfs.open(test_bucket_name + "/test", "wb") as f:
        f.write(b"hello")
    assert ossfs.cat(test_bucket_name + "/test") == b"hello"
    ossfs.open(test_bucket_name + "/test", "wb").close()
    assert ossfs.info(test_bucket_name + "/test")["Size"] == 0


def test_write_large(ossfs):
    "flush() chunks buffer when processing large singular payload"
    mb = 2 ** 20
    payload_size = int(2.5 * 5 * mb)
    payload = b"0" * payload_size

    with ossfs.open(test_bucket_name + "/test", "wb") as fd:
        fd.write(payload)

    assert ossfs.cat(test_bucket_name + "/test") == payload
    assert ossfs.info(test_bucket_name + "/test")["Size"] == payload_size


def test_write_limit(ossfs):
    "flush() respects part_max when processing large singular payload"
    mb = 2 ** 20
    block_size = 15 * mb
    payload_size = 44 * mb
    payload = b"0" * payload_size

    with ossfs.open(
        test_bucket_name + "/test", "wb", blocksize=block_size
    ) as fd:
        fd.write(payload)

    assert ossfs.cat(test_bucket_name + "/test") == payload

    assert ossfs.info(test_bucket_name + "/test")["Size"] == payload_size


def test_write_fails(ossfs):
    with pytest.raises(ValueError):
        ossfs.touch(test_bucket_name + "/temp")
        ossfs.open(test_bucket_name + "/temp", "rb").write(b"hello")
    f = ossfs.open(test_bucket_name + "/temp", "wb")
    f.close()
    with pytest.raises(ValueError):
        f.write(b"hello")
    with pytest.raises(FileNotFoundError):
        ossfs.open("nonexistentbucket/temp", "wb").close()


def test_write_blocks(ossfs):
    with ossfs.open(test_bucket_name + "/temp", "wb") as f:
        f.write(b"a" * 2 * 2 ** 20)
        assert f.buffer.tell() == 2 * 2 ** 20
        f.flush()
        assert f.buffer.tell() == 2 * 2 ** 20
        f.write(b"a" * 2 * 2 ** 20)
        f.write(b"a" * 2 * 2 ** 20)
    assert ossfs.info(test_bucket_name + "/temp")["Size"] == 6 * 2 ** 20
    with ossfs.open(
        test_bucket_name + "/temp", "wb", block_size=10 * 2 ** 20
    ) as f:
        f.write(b"a" * 15 * 2 ** 20)
        assert f.buffer.tell() == 0
    assert ossfs.info(test_bucket_name + "/temp")["Size"] == 15 * 2 ** 20


def test_readline(ossfs):
    all_items = files.items()
    for k, data in all_items:
        with ossfs.open("/".join([test_bucket_name, k]), "rb") as f:
            result = f.readline()
            expected = data.split(b"\n")[0] + (
                b"\n" if data.count(b"\n") else b""
            )
            assert result == expected


def test_readline_empty(ossfs):
    data = b""
    with ossfs.open(test_file_a, "wb") as f:
        f.write(data)
    with ossfs.open(test_file_a, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(ossfs):
    data = b"ab\n" + b"a" * (10 * 2 ** 20) + b"\nab"
    with ossfs.open(test_file_a, "wb") as f:
        f.write(data)
    with ossfs.open(test_file_a, "rb") as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (10 * 2 ** 20) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(ossfs):
    expected = files["LICENSE"].split(b"\n")[0] + b"\n"
    with ossfs.open(test_bucket_name + "/LICENSE") as f:
        result = next(f)
        assert result == expected


def test_iterable(ossfs):
    data = b"abc\n123"
    with ossfs.open(test_file_a, "wb") as f:
        f.write(data)
    with ossfs.open(test_file_a) as f, io.BytesIO(data) as g:
        for fromossfs, fromio in zip(f, g):
            assert fromossfs == fromio
        f.seek(0)
        assert f.readline() == b"abc\n"
        assert f.readline() == b"123"
        f.seek(1)
        assert f.readline() == b"bc\n"

    with ossfs.open(test_file_a) as f:
        out = list(f)
    with ossfs.open(test_file_a) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_readable(ossfs):
    with ossfs.open(test_file_a, "wb") as f:
        assert not f.readable()

    with ossfs.open(test_file_a, "rb") as f:
        assert f.readable()


def test_seekable(ossfs):
    with ossfs.open(test_file_a, "wb") as f:
        assert not f.seekable()

    with ossfs.open(test_file_a, "rb") as f:
        assert f.seekable()


def test_writable(ossfs):
    with ossfs.open(test_file_a, "wb") as f:
        assert f.writable()

    with ossfs.open(test_file_a, "rb") as f:
        assert not f.writable()


def test_append(ossfs):
    data = b"start"
    ossfs.rm(test_file_b)
    with ossfs.open(test_file_b, "ab") as f:
        f.write(data)
        assert f.tell() == len(data)  # append, no write, small file
    with ossfs.open(test_file_b, "ab") as f:
        f.write(b"extra")  # append, write, small file
    assert ossfs.cat(test_file_b) == data + b"extra"

    with ossfs.open(test_file_a, "wb") as f:
        f.write(b"a" * 10 * 2 ** 20)
    with ossfs.open(test_file_a, "ab") as f:
        pass  # append, no write, big file
    assert ossfs.cat(test_file_a) == b"a" * 10 * 2 ** 20

    with ossfs.open(test_file_a, "ab") as f:
        f._initiate_upload()
        f.write(b"extra")  # append, small write, big file
        assert f.tell() == 10 * 2 ** 20 + 5
    assert ossfs.cat(test_file_a) == b"a" * 10 * 2 ** 20 + b"extra"

    with ossfs.open(test_file_a, "ab") as f:
        f.write(b"b" * 10 * 2 ** 20)  # append, big write, big file
        assert f.tell() == 20 * 2 ** 20 + 5
    assert (
        ossfs.cat(test_file_a)
        == b"a" * 10 * 2 ** 20 + b"extra" + b"b" * 10 * 2 ** 20
    )


def test_bigger_than_block_read(ossfs):
    with ossfs.open(test_bucket_name + "/number", "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(4)
            out.append(data)
            if len(data) == 0:
                break
        print(out)
    assert b"".join(out) == b"1234567890\n"


def test_array(ossfs):

    data = array("B", [65] * 1000)

    with ossfs.open(test_file_a, "wb") as f:
        f.write(data)

    with ossfs.open(test_file_a, "rb") as f:
        out = f.read()
        assert out == b"A" * 1000


def test_text_io__stream_wrapper_works(ossfs):
    """Ensure using TextIOWrapper works."""
    with ossfs.open(test_file_c, "wb") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af".encode("utf-16-le"))

    with ossfs.open(test_file_c, "rb") as fd:
        with io.TextIOWrapper(fd, "utf-16-le") as stream:
            assert stream.readline() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__basic(ossfs):
    """Text mode is now allowed."""

    with ossfs.open(test_file_c, "w", encoding="utf-8") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af")

    with ossfs.open(test_file_c, "r", encoding="utf-8") as fd:
        assert fd.read() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__override_encoding(ossfs):
    """Allow overriding the default text encoding."""
    ossfs.mkdir("bucket")

    with ossfs.open(test_file_c, "w", encoding="ibm500") as fd:
        fd.write("Hello, World!")

    with ossfs.open(test_file_c, "r", encoding="ibm500") as fd:
        assert fd.read() == "Hello, World!"


def test_readinto(ossfs):

    with ossfs.open(test_file_c, "wb") as fd:
        fd.write(b"Hello, World!")

    contents = bytearray(15)

    with ossfs.open(test_file_c, "rb") as fd:
        assert fd.readinto(contents) == 13

    assert contents.startswith(b"Hello, World!")


def test_autocommit():
    pass


def test_seek_reads(ossfs):
    fn = test_bucket_name + "/myfile"
    with ossfs.open(fn, "wb") as f:
        f.write(b"a" * 175627146)
    with ossfs.open(fn, "rb", blocksize=100) as f:
        f.seek(175561610)
        f.read(65536)

        f.seek(4)
        size = 17562198
        d2 = f.read(size)
        assert len(d2) == size

        f.seek(17562288)
        size = 17562187
        d3 = f.read(size)
        assert len(d3) == size
