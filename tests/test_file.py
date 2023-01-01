"""
Test all OSSFile related methods
"""
# pylint:disable=invalid-name
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
import io
import os

import pytest

from .conftest import LICENSE_PATH, NUMBERS


def test_simple(ossfs, test_path):
    file = test_path + "/test_simple/file"
    data = os.urandom(10 * 2**20)

    with ossfs.open(file, "wb") as f:
        f.write(data)

    with ossfs.open(file, "rb") as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_seek(ossfs, test_path):
    file = test_path + "/test_seek/file"
    with ossfs.open(file, "wb") as f:
        f.write(b"123")

    with ossfs.open(file) as f:
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


def test_read_small(ossfs, number_file):
    with ossfs.open(number_file, "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(2)
            if data == b"":
                break
            out.append(data)
        assert ossfs.cat(number_file) == b"".join(out)


def test_read_ossfs_block(ossfs, license_file, number_file):
    with open(LICENSE_PATH, "rb") as f_r:
        data = f_r.read()
    lines = io.BytesIO(data).readlines()
    assert ossfs.read_block(license_file, 0, 10, b"\n") == lines[0]
    assert ossfs.read_block(license_file, 40, 10, b"\n") == lines[1]
    assert ossfs.read_block(license_file, 0, 80, b"\n") == lines[0] + lines[1]
    assert ossfs.read_block(license_file, 0, 120, b"\n") == b"".join(
        [lines[0], lines[1], lines[2]]
    )

    lines = io.BytesIO(NUMBERS).readlines()
    assert len(ossfs.read_block(number_file, 0, 5)) == 5
    assert len(ossfs.read_block(number_file, 4, 150)) == len(NUMBERS) - 4
    assert ossfs.read_block(number_file, 20, 25) == b""

    assert ossfs.read_block(number_file, 5, None) == ossfs.read_block(
        number_file, 5, 25
    )


@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_write(ossfs, test_path, size):
    file = test_path + "/test_write/file"
    data = os.urandom(size)
    with ossfs.open(file, "wb") as f:
        f.write(data)
    assert ossfs.cat(file) == data
    assert ossfs.info(file)["Size"] == len(data)
    ossfs.open(file, "wb").close()
    assert ossfs.info(file)["Size"] == 0


def test_write_fails(ossfs, test_path):
    file = test_path + "/test_write_fails/temp"
    ossfs.touch(file)
    with pytest.raises(ValueError):
        ossfs.open(file, "rb").write(b"hello")
    f = ossfs.open(file, "wb")
    f.close()
    with pytest.raises(ValueError):
        f.write(b"hello")
    with pytest.raises(FileNotFoundError):
        ossfs.open("nonexistentbucket/temp", "wb").close()


def test_write_blocks(ossfs, test_path):
    file = test_path + "/test_write_blocks/temp"
    with ossfs.open(file, "wb") as f:
        f.write(os.urandom(2 * 2**20))
        assert f.buffer.tell() == 2 * 2**20
        f.flush()
        assert f.buffer.tell() == 2 * 2**20
        f.write(os.urandom(2 * 2**20))
        f.write(os.urandom(2 * 2**20))
    assert ossfs.info(file)["Size"] == 6 * 2**20
    with ossfs.open(file, "wb", block_size=10 * 2**20) as f:
        f.write(os.urandom(15 * 2**20))
        assert f.buffer.tell() == 0
    assert ossfs.info(file)["Size"] == 15 * 2**20


def test_readline(ossfs, number_file, license_file):
    with ossfs.open("/".join([number_file]), "rb") as f_r:
        result = f_r.readline()
        expected = NUMBERS
        assert result == expected

    with ossfs.open("/".join([license_file]), "rb") as f_r, open(
        LICENSE_PATH, "rb"
    ) as f_l:
        result = f_r.readline()
        expected = f_l.readline()
        assert result == expected


def test_readline_empty(ossfs, test_path):
    file = test_path + "/test_readline_empty/empty"
    data = b""
    with ossfs.open(file, "wb") as f:
        f.write(data)
    with ossfs.open(file, "rb") as f:
        result = f.readline()
        assert result == data


def test_readline_blocksize(ossfs, test_path):
    test_file_a = test_path + "/test_readline_blocksize/a"
    data = b"ab\n" + b"a" * (10 * 2**20) + b"\nab"
    with ossfs.open(test_file_a, "wb") as f:
        f.write(data)
    with ossfs.open(test_file_a, "rb") as f:
        result = f.readline()
        expected = b"ab\n"
        assert result == expected

        result = f.readline()
        expected = b"a" * (10 * 2**20) + b"\n"
        assert result == expected

        result = f.readline()
        expected = b"ab"
        assert result == expected


def test_next(ossfs, license_file):
    with open(LICENSE_PATH, "rb") as f_l, ossfs.open(license_file) as f_r:
        expected = f_l.readline()
        result = next(f_r)
        assert result == expected


def test_iterable(ossfs, test_path):
    file = test_path + "/test_iterable/file"
    data = b"abc\n123"
    with ossfs.open(file, "wb") as f:
        f.write(data)
    with ossfs.open(file) as f, io.BytesIO(data) as g:
        for fromossfs, fromio in zip(f, g):
            assert fromossfs == fromio
        f.seek(0)
        assert f.readline() == b"abc\n"
        assert f.readline() == b"123"
        f.seek(1)
        assert f.readline() == b"bc\n"

    with ossfs.open(file) as f:
        out = list(f)
    with ossfs.open(file) as f:
        out2 = f.readlines()
    assert out == out2
    assert b"".join(out) == data


def test_file_status(ossfs, test_path):
    file = test_path + "/test_file_status/file"
    with ossfs.open(file, "wb") as f:
        assert not f.readable()
        assert not f.seekable()
        assert f.writable()

    with ossfs.open(file, "rb") as f:
        assert f.readable()
        assert f.seekable()
        assert not f.writable()


@pytest.mark.parametrize("data_size", [0, 20, 10 * 2**20])
@pytest.mark.parametrize("append_size", [0, 20, 10 * 2**20])
def test_append(ossfs, test_path, data_size, append_size):
    file = test_path + f"/test_append/file_{data_size}_{append_size}"
    data = os.urandom(data_size)
    extra = os.urandom(append_size)
    with ossfs.open(file, "wb") as f:
        f.write(data)
    assert ossfs.cat(file) == data
    with ossfs.open(file, "ab") as f:
        f.write(extra)  # append, write, small file
    assert ossfs.cat(file) == data + extra


def test_bigger_than_block_read(ossfs, number_file):
    with ossfs.open(number_file, "rb", block_size=3) as f:
        out = []
        while True:
            data = f.read(4)
            out.append(data)
            if len(data) == 0:
                break
    assert b"".join(out) == b"1234567890\n"


def test_text_io__stream_wrapper_works(ossfs, test_path):
    """Ensure using TextIOWrapper works."""
    file = test_path + "/test_text_io__stream_wrapper_works/file"
    with ossfs.open(file, "wb") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af".encode("utf-16-le"))

    with ossfs.open(file, "rb") as fd:
        with io.TextIOWrapper(fd, "utf-16-le") as stream:
            assert stream.readline() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__basic(ossfs, test_path):
    """Text mode is now allowed."""
    file = test_path + "/test_text_io__basic/file"
    with ossfs.open(file, "w", encoding="utf-8") as fd:
        fd.write("\u00af\\_(\u30c4)_/\u00af")

    with ossfs.open(file, "r", encoding="utf-8") as fd:
        assert fd.read() == "\u00af\\_(\u30c4)_/\u00af"


def test_text_io__override_encoding(ossfs, test_path):
    """Allow overriding the default text encoding."""
    file = test_path + "/test_text_io__override_encoding/file"

    with ossfs.open(file, "w", encoding="ibm500") as fd:
        fd.write("Hello, World!")

    with ossfs.open(file, "r", encoding="ibm500") as fd:
        assert fd.read() == "Hello, World!"


def test_readinto(ossfs, test_path):
    file = test_path + "/test_readinto/file"

    with ossfs.open(file, "wb") as fd:
        fd.write(b"Hello, World!")

    contents = bytearray(15)

    with ossfs.open(file, "rb") as fd:
        assert fd.readinto(contents) == 13

    assert contents.startswith(b"Hello, World!")


def test_seek_reads(ossfs, test_path):
    file = test_path + "/test_seek_reads/file"
    with ossfs.open(file, "wb") as f:
        f.write(os.urandom(5627146))
    with ossfs.open(file, "rb", blocksize=100) as f:
        f.seek(5561610)
        f.read(65536)

        f.seek(4)
        size = 562198
        d2 = f.read(size)
        assert len(d2) == size

        f.seek(562288)
        size = 562187
        d3 = f.read(size)
        assert len(d3) == size
