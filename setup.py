# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import pathlib


here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="ptool",
    version="0.2.0",
    description="Analyse project data in pool directory at various sites",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    packages=find_packages(include=["ptool"]),
    install_requires=[
        "click",
        "pyyaml",
        "humanize",
        "tabulate",
        "pandas",
        "pyarrow",
        "imohash",
        "paramiko",
        "questionary",
        "ruamel-yaml",
        "tqdm",
    ],
    entry_points="""
        [console_scripts]
        ptool=ptool.cli:cli
    """,
    classifiers=[
        "Development Status :: 0.2.0",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    author="Pavan Siligam",
    author_email="pavan.siligam@gmail.com",
    license="MIT",
    url="https://gitlab.awi.de/hpc/pool",
)
