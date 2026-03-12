#!/usr/bin/env python3
"""
Setup script for distributed_NBRewind package.
"""

from setuptools import setup, find_packages

setup(
    name="taskvine_replay",
    version="0.1.0",
    description="TaskVine with automatic task caching and replay",
    packages=find_packages(),
    python_requires=">=3.1",
    install_requires=[
        # ndcctools should already be installed
    ],
)
