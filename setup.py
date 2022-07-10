"""Setup."""

from setuptools import find_packages, setup

setup(
    name="lastfmrec",
    version="0.2.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "requests==2.27.1",
        "psycopg2-binary==2.9.3",
        "tqdm==4.63.0",
        "click==8.1.3",
    ],
    extras_require=dict(
        dbt=["dbt-postgres==1.1.0"],
        test=[
            "black==22.6.0",
            "pytest==7.1.2",
        ],
    ),
)
