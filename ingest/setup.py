"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_ingest" / "version")
    .read_text()
    .strip()
)


setup(
    name="moomoo-ingest",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg[binary]==3.1.*",
        "sqlalchemy==2.0.*",
        "pgvector==0.1.*",
        "requests==2.*",
        "tqdm==4.65.*",
        "click==8.1.*",
        "tenacity==8.2.2",
        "mutagen==1.46.0",
        "pylistenbrainz==0.5.1",
        "musicbrainzngs==0.7.1",
    ],
    extras_require=dict(
        test=[
            "ruff==0.7.*",
            "pytest==7.2.2",
            "requests-mock==1.11.0",
            "pytest-postgresql==5.0.0",
        ]
    ),
    entry_points={"console_scripts": ["moomoo-ingest=moomoo_ingest.cli:cli"]},
    package_data={
        "moomoo_ingest": ["version"],
    },
)
