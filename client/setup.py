"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_client" / "version")
    .read_text()
    .strip()
)

readme = (Path(__file__).resolve().parent / "readme.md").read_text()


setup(
    name="moomoo-client",
    version=version,
    author_email="nolanbconaway@gmail.com",
    url="https://github.com/nolanbconaway/moomoo/tree/main/client",
    description="A command-line client installation for moomoo.",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "click==8.*",
        "xspf-lib==0.3.*",
        "httpx==0.28.*",
        "toga==0.5.*",
        "structlog==25.*",
    ],
    extras_require=dict(
        test=[
            "ruff==0.14.10",
            "pytest==9.0.2",
            "pytest-httpx==0.36.0",
            "pytest-asyncio==1.3.0",
        ]
    ),
    entry_points={"console_scripts": ["moomoo-client=moomoo_client.cli.cli:cli"]},
    package_data={"moomoo_client": ["version"]},
)
