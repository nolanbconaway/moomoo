"""Setup."""

from setuptools import find_packages, setup

setup(
    name="moomoo",
    version="0.1.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg2-binary==2.9.3",
        "tenacity==8.1.0",
        "tqdm==4.63.0",
        "click==8.1.3",
        "mutagen==1.46.0",
        "pylistenbrainz==0.5.1",
    ],
    extras_require=dict(
        dbt=["dbt-postgres==1.1.0"],
        test=[
            "black==22.6.0",
            "pytest==7.1.2",
            "sqlfluff==1.4.5",
            "sqlfluff-templater-dbt==1.4.5",
        ],
    ),
    entry_points={"console_scripts": ["moomoo=moomoo.cli:cli"]},
)
