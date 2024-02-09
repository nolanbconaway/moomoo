"""CLI commands for playlist generation."""

import asyncio
from pathlib import Path

import click

from ..http import PlaylistRequester

# shared among most commands
OPTION_OUT = click.option(
    "--out", default="json", type=click.Choice(["xml", "json", "strawberry"])
)


@click.group()
def cli():
    """Playlist commands."""
    pass


@cli.command("from-path")
@click.argument(
    "paths", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True
)
@OPTION_OUT
def playlist_from_path(paths: list[Path], out: str):
    """Get a playlist from a path."""
    requester = PlaylistRequester()
    playlist = asyncio.run(requester.request_playlist_from_path(paths))
    playlist.render(out)


@cli.command("loved")
@click.argument(
    "username", type=str, nargs=1, required=True, envvar="LISTENBRAINZ_USERNAME"
)
@OPTION_OUT
def playlist_loved_tracks(username, out: str):
    """Get the loved tracks of a user."""
    requester = PlaylistRequester()
    playlist = asyncio.run(requester.request_loved_tracks(username))
    playlist.render(out)


@cli.command("revisit-releases")
@click.argument(
    "username", type=str, nargs=1, required=True, envvar="LISTENBRAINZ_USERNAME"
)
@OPTION_OUT
def playlist_revisit_releases(username, out: str):
    """Get releases to revisit."""
    requester = PlaylistRequester()
    playlists = asyncio.run(requester.request_revisit_releases(username))

    # give users a choice of playlists and wait for input
    click.echo("Choose a playlist:")
    for i, playlist in enumerate(playlists):
        click.echo(f"{i}: {playlist.description}")

    choice = click.prompt("Choice", type=int)

    playlists[choice].render(out)


@cli.command("suggest-artists")
@click.argument(
    "username", type=str, nargs=1, required=True, envvar="LISTENBRAINZ_USERNAME"
)
@OPTION_OUT
def playlist_suggested_artists(username: str, out: str):
    """Get playlists of suggested artists."""
    requester = PlaylistRequester()
    playlists = asyncio.run(requester.request_user_artist_suggestions(username))

    # give users a choice of playlists and wait for input
    click.echo("Choose a playlist:")
    for i, playlist in enumerate(playlists):
        click.echo(f"{i}: {playlist.description}")

    choice = click.prompt("Choice", type=int)

    playlists[choice].render(out)


if __name__ == "__main__":
    cli()
