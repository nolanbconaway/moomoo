"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_navidrome" / "version").read_text().strip()
)

# setup for installing moomoo-playlist from a git repository
# TODO: get the url from shell? grab from a tagged release?
playlist_commit = "ac338df2ab58c5c3a2f6ffd47e4b38756bb89dfb"
playlist_url = "https://github.com/nolanbconaway/moomoo.git"

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
        "httpx==0.28.*",
        "loguru==0.7.*",
        "pydantic==2.13.*",
        f"moomoo-playlist @ git+{playlist_url}@{playlist_commit}#subdirectory=playlist",
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
