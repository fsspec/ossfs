# Sssfs

Fsspec filesystem for Alibaba Cloud (Aliyun) Object Storage System (OSS)

## Install

```bash
pip install ossfs
```

or

```bash
conda install -c conda-forge ossfs
```

## Quick Start

Simple locate and read a file:

```python
>>> import ossfs
>>> fs = ossfs.OSSFileSystem(endpoint='http://oss-cn-hangzhou.aliyuncs.com')
>>> fs.ls('/dvc-test-anonymous/LICENSE')
[{'name': '/dvc-test-anonymous/LICENSE',
  'Key': '/dvc-test-anonymous/LICENSE',
  'type': 'file',
  'size': 11357,
  'Size': 11357,
  'StorageClass': 'OBJECT',
  'LastModified': 1622761222}]
>>> with fs.open('/dvc-test-anonymous/LICENSE') as f:
...     print(f.readline())
b'                                 Apache License\n'
```