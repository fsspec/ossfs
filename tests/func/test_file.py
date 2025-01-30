"""
Test all OSSFile related methods
"""

import io
import os
from typing import TYPE_CHECKING, Union

import pytest

from tests.conftest import LICENSE_PATH, NUMBERS, bucket_relative_path, function_name

if TYPE_CHECKING:
    from oss2 import Bucket

    from ossfs import AioOSSFileSystem, OSSFileSystem


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_simple_read(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    object_name = f"{test_path}/{function_name(ossfs)}"
    data = os.urandom(10 * 2**20)
    bucket.put_object(bucket_relative_path(object_name), data)

    with ossfs.open(object_name, "rb") as f_rb:
        out = f_rb.read(len(data))
    assert len(data) == len(out)
    assert out == data


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_simple_write(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    object_name = f"{test_path}/{function_name(ossfs)}"
    data = os.urandom(10 * 2**20)

    with ossfs.open(object_name, "wb") as f_wb:
        f_wb.write(data)

    out = bucket.get_object(bucket_relative_path(object_name)).read()
    assert out == data


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_seek(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    object_name = f"{test_path}/{function_name(ossfs)}"
    bucket.put_object(bucket_relative_path(object_name), b"123")

    with ossfs.open(object_name) as f_seek:
        f_seek.seek(1000)
        with pytest.raises(ValueError):
            f_seek.seek(-1)
        with pytest.raises(ValueError):
            f_seek.seek(-5, 2)
        with pytest.raises(ValueError):
            f_seek.seek(0, 10)
        f_seek.seek(0)
        assert f_seek.read(1) == b"1"
        f_seek.seek(0)
        assert f_seek.read(1) == b"1"
        f_seek.seek(3)
        assert f_seek.read(1) == b""
        f_seek.seek(-1, 2)
        assert f_seek.read(1) == b"3"
        f_seek.seek(-1, 1)
        f_seek.seek(-1, 1)
        assert f_seek.read(1) == b"2"
        for i in range(4):
            assert f_seek.seek(i) == i


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_read_small(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    number_file: str,
    bucket: "Bucket",
):
    with ossfs.open(number_file, "rb", block_size=3) as f_rb:
        out = []
        while True:
            data = f_rb.read(2)
            if data == b"":
                break
            out.append(data)
        assert bucket.get_object(bucket_relative_path(number_file)).read() == b"".join(
            out
        )


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_read_ossfs_block(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    license_file: str,
    number_file: str,
):
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


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
@pytest.mark.parametrize("size", [2**10, 2**20, 10 * 2**20])
def test_write(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_path: str,
    size: int,
    bucket: "Bucket",
):
    object_name = f"{test_path}/{function_name(ossfs)}"
    data = os.urandom(size)
    with ossfs.open(object_name, "wb") as f_wb:
        f_wb.write(data)
    out = bucket.get_object(bucket_relative_path(object_name)).read()
    assert data == out
    assert ossfs.info(object_name)["Size"] == len(data)
    ossfs.open(object_name, "wb").close()
    assert ossfs.info(object_name)["Size"] == 0


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_write_fails(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    object_name = f"{test_path}/{function_name(ossfs)}"
    ossfs.touch(object_name)
    with pytest.raises(ValueError):
        ossfs.open(object_name, "rb").write(b"hello")
    f_wb = ossfs.open(object_name, "wb")
    f_wb.close()
    with pytest.raises(ValueError):
        f_wb.write(b"hello")
    with pytest.raises(FileNotFoundError):
        ossfs.open("nonexistentbucket/temp", "wb").close()


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_write_blocks(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str, bucket: "Bucket"
):
    object_name = f"{test_path}/{function_name(ossfs)}"
    with ossfs.open(object_name, "wb") as f_wb:
        f_wb.write(os.urandom(2 * 2**20))
        assert f_wb.buffer.tell() == 2 * 2**20
        f_wb.flush()
        assert f_wb.buffer.tell() == 2 * 2**20
        f_wb.write(os.urandom(2 * 2**20))
        f_wb.write(os.urandom(2 * 2**20))
    out = bucket.get_object(bucket_relative_path(object_name)).read()
    assert len(out) == 6 * 2**20
    with ossfs.open(object_name, "wb", block_size=10 * 2**20) as f_wb:
        f_wb.write(os.urandom(15 * 2**20))
        assert f_wb.buffer.tell() == 0
    out = bucket.get_object(bucket_relative_path(object_name)).read()
    assert len(out) == 15 * 2**20


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_readline(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    number_file: str,
    license_file: str,
):
    with ossfs.open(f"{number_file}", "rb") as f_rb:
        result = f_rb.readline()
        expected = NUMBERS
        assert result == expected

    with ossfs.open(f"{license_file}", "rb") as f_rb, open(LICENSE_PATH, "rb") as f_l:
        result = f_rb.readline()
        expected = f_l.readline()
        assert result == expected


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_readline_empty(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str
):
    file = f"{test_path}/{function_name(ossfs)}/empty"
    data = b""
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(data)
    with ossfs.open(file, "rb") as f_rb:
        result = f_rb.readline()
        assert result == data


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_readline_blocksize(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str
):
    test_file_a = f"{test_path}/{function_name(ossfs)}/a"
    data = b"ab\n" + b"a" * (10 * 2**20) + b"\nab"
    with ossfs.open(test_file_a, "wb") as f_wb:
        f_wb.write(data)
    with ossfs.open(test_file_a, "rb") as f_rb:
        result = f_rb.readline()
        expected = b"ab\n"
        assert result == expected

        result = f_rb.readline()
        expected = b"a" * (10 * 2**20) + b"\n"
        assert result == expected

        result = f_rb.readline()
        expected = b"ab"
        assert result == expected


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_next(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], license_file: str):
    with open(LICENSE_PATH, "rb") as f_rb, ossfs.open(license_file) as f_r:
        expected = f_rb.readline()
        result = next(f_r)
        assert result == expected


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_iterable(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    file = f"{test_path}/{function_name(ossfs)}/file"
    data = b"abc\n123"
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(data)
    with ossfs.open(file) as f_r, io.BytesIO(data) as bytes_io:
        for fromossfs, fromio in zip(f_r, bytes_io):
            assert fromossfs == fromio
        f_r.seek(0)
        assert f_r.readline() == b"abc\n"
        assert f_r.readline() == b"123"
        f_r.seek(1)
        assert f_r.readline() == b"bc\n"

    with ossfs.open(file) as f_r:
        out = list(f_r)
    with ossfs.open(file) as f_r:
        out2 = f_r.readlines()
    assert out == out2
    assert b"".join(out) == data


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_file_status(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    file = f"{test_path}/{function_name(ossfs)}/file"
    with ossfs.open(file, "wb") as f_wb:
        assert not f_wb.readable()
        assert not f_wb.seekable()
        assert f_wb.writable()

    with ossfs.open(file, "rb") as f_rb:
        assert f_rb.readable()
        assert f_rb.seekable()
        assert not f_rb.writable()


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
@pytest.mark.parametrize("data_size", [0, 20, 10 * 2**20])
@pytest.mark.parametrize("append_size", [0, 20, 10 * 2**20])
def test_append(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"],
    test_path: str,
    data_size: int,
    append_size: int,
):
    file = f"{test_path}/{function_name(ossfs)}/file_{data_size}_{append_size}"
    data = os.urandom(data_size)
    extra = os.urandom(append_size)
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(data)
    assert ossfs.cat(file) == data
    with ossfs.open(file, "ab") as f_ab:
        f_ab.write(extra)  # append, write, small file
    assert ossfs.cat(file) == data + extra


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_bigger_than_block_read(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], number_file: str
):
    with ossfs.open(number_file, "rb", block_size=3) as f_rb:
        out = []
        while True:
            data = f_rb.read(4)
            out.append(data)
            if len(data) == 0:
                break
    assert b"".join(out) == NUMBERS


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_text_io__stream_wrapper_works(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str
):
    """Ensure using TextIOWrapper works."""
    file = f"{test_path}/{function_name(ossfs)}/file"
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write("\u00af\\_(\u30c4)_/\u00af".encode("utf-16-le"))

    with ossfs.open(file, "rb") as f_rb:
        with io.TextIOWrapper(f_rb, "utf-16-le") as stream:
            assert stream.readline() == "\u00af\\_(\u30c4)_/\u00af"


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_text_io__basic(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str
):
    """Text mode is now allowed."""
    file = f"{test_path}/{function_name(ossfs)}/file"
    with ossfs.open(file, "w", encoding="utf-8") as f_w:
        f_w.write("\u00af\\_(\u30c4)_/\u00af")

    with ossfs.open(file, "r", encoding="utf-8") as f_r:
        assert f_r.read() == "\u00af\\_(\u30c4)_/\u00af"


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_text_io__override_encoding(
    ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str
):
    """Allow overriding the default text encoding."""
    file = f"{test_path}/{function_name(ossfs)}/file"

    with ossfs.open(file, "w", encoding="ibm500") as f_w:
        f_w.write("Hello, World!")

    with ossfs.open(file, "r", encoding="ibm500") as f_r:
        assert f_r.read() == "Hello, World!"


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_readinto(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    file = f"{test_path}/{function_name(ossfs)}/file"

    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(b"Hello, World!")

    contents = bytearray(15)

    with ossfs.open(file, "rb") as f_wb:
        assert f_wb.readinto(contents) == 13

    assert contents.startswith(b"Hello, World!")


@pytest.mark.parametrize("ossfs", ["async", "sync"], indirect=True)
def test_seek_reads(ossfs: Union["OSSFileSystem", "AioOSSFileSystem"], test_path: str):
    file = f"{test_path}/{function_name(ossfs)}/file"
    with ossfs.open(file, "wb") as f_wb:
        f_wb.write(os.urandom(5627146))
    with ossfs.open(file, "rb", blocksize=100) as f_rb:
        f_rb.seek(5561610)
        f_rb.read(65536)

        f_rb.seek(4)
        size = 562198
        data_2 = f_rb.read(size)
        assert len(data_2) == size

        f_rb.seek(562288)
        size = 562187
        data_3 = f_rb.read(size)
        assert len(data_3) == size
