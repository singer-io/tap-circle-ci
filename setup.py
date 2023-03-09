#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-circle-ci",
    version="0.1.2",
    description="Singer.io tap for extracting data from circle ci",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_circle_ci"],
    install_requires=[
        "singer-python==5.13.0",
        "requests==2.20.0",
    ],
    extras_require={"dev": ["pylint", "pytest", "mock"]},
    entry_points="""
    [console_scripts]
    tap-circle-ci=tap_circle_ci:main
    """,
    packages=["tap_circle_ci"],
    package_data={"schemas": ["tap_circle_ci/schemas/*.json"]},
    include_package_data=True,
)
