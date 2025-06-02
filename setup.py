#!/usr/bin/env python

# Importing necessary functions from setuptools package.
from setuptools import setup

# Importing path manipulation utilities.
from os.path import dirname, abspath, join

# Setting the path to the README file which is used as the long description for the package.
long_description_path = join(dirname(abspath(__file__)), "README.md")
# Reading the content of README.md to use as the long description.
long_description = open(long_description_path, encoding="utf-8").read()

# Calling the setup function to handle the packaging of the module.
setup(
    name="pretty-csv-diff",  # Name of the package.
    long_description=long_description,  # Long description read from README.md.
    long_description_content_type="text/markdown",  # Specifying the format of the long description.
    url="https://github.com/eternity8/pretty-csv-diff",  # URL of the project's homepage.
    license="Apache License 2.0",  # License type for the package.
    version="0.1.1",
    packages=[
        "pretty_csv_diff",  # List of all packages that are part of this distribution.
    ],
    entry_points={
        "console_scripts": [
            "pretty-csv-diff = pretty_csv_diff.__main__:main",  # Entry point for the console script.
        ],
    },
    classifiers=[
        # Metadata classifiers to categorize the project.
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
)
