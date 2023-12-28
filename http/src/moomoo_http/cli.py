"""Cli group for moomoo."""

import logging
from pathlib import Path

import click
import waitress

from .app import create_app


@click.group()
def cli():
    """Cli group for moomoo/http."""
    pass


@cli.command()
def version():
    """Get the version of moomoo/http."""
    click.echo((Path(__file__).resolve().parent / "version").read_text().strip())


@cli.command()
@click.option("--host", envvar="MOOMOO_HOST", default="127.0.0.1")
@click.option("--port", envvar="MOOMOO_PORT", default=5000, type=int)
def serve(host: str, port: int) -> None:
    """Run the moomoo http server."""
    app = create_app()
    logger = logging.getLogger("waitress")
    logger.setLevel(logging.INFO)

    click.echo(f"Starting moomoo http server on {host}:{port}")
    waitress.serve(app, host=host, port=port)


if __name__ == "__main__":
    cli()
