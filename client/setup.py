"""Setup."""

from pathlib import Path

from setuptools import find_packages, setup

version = (
    (Path(__file__).resolve().parent / "src" / "moomoo_client" / "version")
    .read_text()
    .strip()
)


setup(
    name="moomoo-client",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "click==8.*",
        "xspf-lib==0.3.*",
        "requests==2.*",
    ],
    extras_require=dict(
        test=[
            "black==23.10.0",
            "ruff==0.1.1",
            "pytest==7.2.2",
            "requests-mock==1.11.0",
        ]
    ),
    entry_points={"console_scripts": ["moomoo-client=moomoo_client.cli:cli"]},
    package_data={"moomoo-client": ["version"]},
)
