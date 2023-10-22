"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo" / "version").read_text().strip()
)

extras_require = dict(
    test=[
        "black==23.1.0",
        "pytest==7.2.2",
        "pytest-postgresql==5.0.0",
        "pytest-xprocess==0.22.2",
    ],
    ml=[
        # heavy deps for ml inference
        "librosa==0.10.*",
        "transformers==4.28",
        "torch==2.*",
        "torchaudio==2.*",
        "nnAudio==0.3.*",
    ],
    ingest=[
        # for ingesting musicbrainz, local files, and listenbrainz
        "tenacity==8.2.2",
        "mutagen==1.46.0",
        "pylistenbrainz==0.5.1",
        "musicbrainzngs==0.7.1",
    ],
    http=["flask==2.3.*", "waitress==2.1.*"],
)

extras_require["all"] = sum(extras_require.values(), [])

setup(
    name="moomoo",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg[binary]==3.1.*",
        "sqlalchemy==2.0.*",
        "tqdm==4.65.0",
        "click==8.1.3",
        "xspf-lib==0.3.*",
        "pgvector==0.1.*",
        "requests==2.*",
    ],
    extras_require=extras_require,
    entry_points={"console_scripts": ["moomoo=moomoo.cli:cli"]},
    package_data={
        "moomoo": ["version", "ml/model-info.json"],
    },
)
