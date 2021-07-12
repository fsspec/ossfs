"""
Modules Distributions.
"""

from setuptools import find_packages, setup

with open("requirements.txt") as f_r:
    tests_requirements = [line.strip() for line in f_r.readlines()]

setup(
    name="ossfs",
    version="2021.7.3",
    description="fsspec filesystem for OSS",
    long_description=open("README.md", "r", encoding="UTF-8").read(),
    author="Yanxiang Gao",
    author_email="gao@iterive.ai",
    download_url="https://github.com/iterative/ossfs",
    license="Apache-2.0 License",
    install_requires=["fsspec>=2021.5.0", "oss2==2.6.1"],
    extras_require={"tests": tests_requirements},
    keywords="oss, fsspec",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(exclude=["tests"]),
)
