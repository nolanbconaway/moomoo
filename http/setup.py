"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_http" / "version")
    .read_text()
    .strip()
)

# setup for installing moomoo-playlist from a git repository
# TODO: get the url from shell? grab from a tagged release?
playlist_commit = "878e7d3277007f34a63739e90220dd64c65b57ee"
playlist_url = "https://github.com/nolanbconaway/moomoo.git"

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
        f"moomoo-playlist @ git+{playlist_url}@{playlist_commit}#subdirectory=playlist",
    ],
    extras_require=dict(
        test=[
            "ruff==0.2.0",
            "pytest==7.4.4",
            "pytest-postgresql==5.0.0",
        ],
    ),
    entry_points={"console_scripts": ["moomoo-http=moomoo_http.cli:cli"]},
    package_data={
        "moomoo_http": ["version"],
    },
)
