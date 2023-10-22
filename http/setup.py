"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_http" / "version")
    .read_text()
    .strip()
)


setup(
    name="moomoo-http",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "flask==2.3.*",
        "waitress==2.1.*",
        "Flask-SQLAlchemy==3.1.*",
        "psycopg[binary]==3.1.*",
    ],
    extras_require=dict(
        test=[
            "black==23.10.0",
            "ruff==0.1.1",
            "pytest==7.4.2",
            "pytest-postgresql==5.0.0",
        ],
    ),
    entry_points={"console_scripts": ["moomoo-http=moomoo_http.cli:cli"]},
    package_data={
        "moomoo_http": ["version"],
    },
)
