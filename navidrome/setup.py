"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_navidrome" / "version")
    .read_text()
    .strip()
)


setup(
    name="moomoo-navidrome",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        # loose requires bc needs to be importable
        "psycopg[binary]==3.1.*",
        "sqlalchemy==2.0.*",
        "pgvector==0.1.*",
        "click==8.*",
    ],
    extras_require=dict(
        test=[
            "ruff==0.15.*",
            "pytest==7.4.4",
            "pytest-postgresql==5.0.0",
        ],
    ),
    entry_points={"console_scripts": ["moomoo-navidrome=moomoo_navidrome.cli:cli"]},
    package_data={
        "moomoo_navidrome": ["version"],
    },
)
