"""Setup."""

from setuptools import find_packages, setup

setup(
    name="lastfmrec",
    version="0.1.3",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "requests==2.27.1",
        "psycopg2-binary==2.9.3",
        "tqdm==4.63.0",
        "dbt-postgres-1.1.0",
    ],
    extras_require=dict(test=["black==22.1.0"]),
)
