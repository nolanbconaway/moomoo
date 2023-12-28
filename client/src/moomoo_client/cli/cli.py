"""A CLI for moomoo client."""
from pathlib import Path

import click

from .playlist import cli as cli_playlist


@click.group()
def cli():
    """Cli group for moomoo client."""
    pass


cli.add_command(cli_playlist, "playlist")


@cli.command()
def version():
    """Get the version of moomoo-client."""
    version = (Path(__file__).resolve().parent.parent / "version").read_text().strip()
    click.echo(version)


@cli.group()
def app():
    """App commands."""
    pass


@app.command("start")
def app_start():
    """Start the moomoo app."""
    from ..gui.app import MyApp

    MyApp().run()


if __name__ == "__main__":
    cli()
