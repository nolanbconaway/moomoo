"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_playlist" / "version")
    .read_text()
    .strip()
)


setup(
    name="moomoo-playlist",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg[binary]==3.1.*",
        "sqlalchemy==2.0.*",
        "pgvector==0.1.*",
        "click==8.1.*",
        "tenacity==8.2.2",
    ],
    extras_require=dict(
        test=[
            "ruff==0.1.14",
            "pytest==7.4.4",
            "pytest-postgresql==5.0.0",
        ],
    ),
    entry_points={"console_scripts": ["moomoo-playlist=moomoo_playlist.cli:cli"]},
    package_data={
        "moomoo_playlist": ["version"],
    },
)
