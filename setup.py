"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo" / "version").read_text().strip()
)

setup(
    name="moomoo",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg2-binary==2.9.5",
        "tenacity==8.2.2",
        "tqdm==4.65.0",
        "click==8.1.3",
        "mutagen==1.46.0",
        "pylistenbrainz==0.5.1",
        "musicbrainzngs==0.7.1",
    ],
    extras_require=dict(
        dbt=["dbt-postgres==1.4.4"],
        test=[
            "black==23.1.0",
            "pytest==7.2.2",
            "sqlfluff==1.4.5",
            "sqlfluff-templater-dbt==1.4.5",
        ],
    ),
    entry_points={"console_scripts": ["moomoo=moomoo.cli:cli"]},
    package_data={"moomoo": ["version"]},
)
