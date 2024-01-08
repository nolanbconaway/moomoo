"""A CLI for moomoo client."""
import os

import click

from ..utils_ import VERSION
from .playlist import cli as cli_playlist


@click.group()
def cli():
    """Cli group for moomoo client."""
    pass


cli.add_command(cli_playlist, "playlist")


@cli.command()
def version():
    """Get the version of moomoo-client."""
    click.echo(VERSION)


@cli.command()
def gui():
    """Open the gui"""
    # check envvars
    for i in ["MOOMOO_HOST", "MOOMOO_MEDIA_LIBRARY", "LISTENBRAINZ_USERNAME"]:
        if i not in os.environ:
            raise ValueError(f"Environment variable {i} not set")

    from ..gui.app import main as gui_main

    gui_main()


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
