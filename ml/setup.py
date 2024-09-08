"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (Path(__file__).resolve().parent / "src" / "moomoo_ml" / "version").read_text().strip()


setup(
    name="moomoo-ml",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "psycopg[binary]==3.1.*",
        "sqlalchemy==2.0.*",
        "tqdm==4.65.0",
        "click==8.1.3",
        "pgvector==0.1.*",
        "librosa==0.10.*",
        "transformers==4.28",
        "torch==2.*",
        "torchaudio==2.*",
        "nnAudio==0.3.*",
        "scikit-learn==1.5.*",
    ],
    extras_require=dict(
        test=[
            "ruff==0.4.4",
            "pytest==7.4.2",
            "pytest-postgresql==5.0.0",
        ],
    ),
    entry_points={"console_scripts": ["moomoo-ml=moomoo_ml.cli:cli"]},
    package_data={
        "moomoo_ml": ["scorer/model-info.json", "conditioner/model-info.json", "version"],
    },
)
