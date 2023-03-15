#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-circle-ci",
    version="0.1.2",
    description="Singer.io tap for extracting data from circle ci",
    author="Sisu Data",
    url="http://sisu.ai",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_circle_ci"],
    install_requires=[
        "singer-python==5.13.0",
        "requests",
    ],
    extras_require={"dev": ["pylint"]},
    entry_points="""
    [console_scripts]
    tap-circle-ci=tap_circle_ci:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data={
        "schemas": ["tap_circle_ci/schemas/*.json"]
    },
    include_package_data=True,
)
