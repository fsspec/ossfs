OSSFS
=====

|PyPI| |Status| |Python Version| |License|

|Tests| |Codecov| |pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/ossfs.svg
   :target: https://pypi.org/project/ossfs/
   :alt: PyPI
.. |Status| image:: https://img.shields.io/pypi/status/ossfs.svg
   :target: https://pypi.org/project/ossfs/
   :alt: Status
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/ossfs
   :target: https://pypi.org/project/ossfs
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/ossfs
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Tests| image:: https://github.com/karajan1001/ossfs/workflows/Tests/badge.svg
   :target: https://github.com/karajan1001/ossfs/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/karajan1001/ossfs/branch/main/graph/badge.svg
   :target: https://app.codecov.io/gh/karajan1001/ossfs
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black

**OSSFS** is a Python-based interface for file systems that enables interaction with
OSS (Object Storage Service). Through **OSSFS**, users can utilize fsspec's standard
API to operate on OSS objects

Installation
------------

You can install *OSSFS* via pip_ from PyPI_:

.. code:: console

   $ pip install ossfs

Up-to-date package also provided through conda-forge distribution:

.. code:: console

   $ conda install -c conda-forge ossfs

Quick Start
------------

Here is a simple example of locating and reading a object in OSS.

.. code:: python

   import ossfs
   fs = ossfs.OSSFileSystem(endpoint='http://oss-cn-hangzhou.aliyuncs.com')
   fs.ls('/dvc-test-anonymous/LICENSE')
   [{'name': '/dvc-test-anonymous/LICENSE',
     'Key': '/dvc-test-anonymous/LICENSE',
     'type': 'file',
     'size': 11357,
     'Size': 11357,
     'StorageClass': 'OBJECT',
     'LastModified': 1622761222}]
   with fs.open('/dvc-test-anonymous/LICENSE') as f:
   ...     print(f.readline())
   b'                                 Apache License\n'

For more use case and apis please refer to the documentation of `fsspec <https://filesystem-spec.readthedocs.io/en/latest/index.html>`_

Async OSSFS
------------

Async **OSSFS** is a variant of ossfs that utilizes the third-party async OSS
backend `aiooss2`_, rather than the official sync one, `oss2`_. Async OSSFS
allows for concurrent calls within bulk operations, such as *cat*, *put*, and
*get* etc even from normal code, and enables the direct use of fsspec in async
code without blocking. The usage of async **OSSFS** is similar to the synchronous
variant; one simply needs to replace **OSSFileSystem** with **AioOSSFileSystem**
need to do is replacing the **OSSFileSystem** with the **AioOSSFileSystem**

.. code:: python

   import ossfs
   fs = ossfs.AioOSSFileSystem(endpoint='http://oss-cn-hangzhou.aliyuncs.com')
   print(fs.cat('/dvc-test-anonymous/LICENSE'))
   b'                                 Apache License\n'
   ...

Although `aiooss2`_ is not officially supported, there are still some
features that are currently lacking. However, in tests involving the
*put*/*get* of 1200 small files, the async version of ossfs ran ten times
faster than the synchronous variant (depending on the pool size of the
concurrency).

+-------------------------------------------+------------------------+
| Task                                      | time cost in (seconds) |
+===========================================+========================+
| put 1200 small files via OSSFileSystem    | 35.2688 (13.53)        |
+-------------------------------------------+------------------------+
| put 1200 small files via AioOSSFileSystem | 2.6060 (1.0)           |
+-------------------------------------------+------------------------+
| get 1200 small files via OSSFileSystem    | 32.9096 (12.63)        |
+-------------------------------------------+------------------------+
| get 1200 small files via AioOSSFileSystem | 3.3497 (1.29)          |
+-------------------------------------------+------------------------+

Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*Ossfs* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _PyPI: https://pypi.org/
.. _file an issue: https://github.com/fsspec/ossfs/issues
.. _aiooss2: https://github.com/karajan1001/aiooss2/
.. _oss2: https://pypi.org/project/oss2/
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
