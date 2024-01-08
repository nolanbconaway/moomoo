"""CLI commands for playlist generation."""
import asyncio
from pathlib import Path

import click

from ..http import PlaylistRequester


def common_options(func):
    """Add common options to a command

    Has same effect as:

        @click.option("--n", default=20, type=int)
        @click.option("--seed", default=1, type=int)
        @click.option("--shuffle", default=True, type=bool)
        @click.option("--out", default="json", type=...

    """
    options = [
        click.option("--n", default=20, type=int),
        click.option("--seed", default=1, type=int),
        click.option("--shuffle", default=True, type=bool),
        click.option(
            "--out", default="json", type=click.Choice(["xml", "json", "strawberry"])
        ),
    ]

    for option in options:
        func = option(func)
    return func


@click.group()
def cli():
    """Playlist commands."""
    pass


@cli.command("from-path")
@click.argument(
    "paths", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True
)
@common_options
def playlist_from_path(paths: list[Path], out: str, n: int, seed: int, shuffle: bool):
    """Get a playlist from a path."""
    requester = PlaylistRequester(tracks=n, seed=seed, shuffle=shuffle)
    playlist = asyncio.run(requester.request_playlist_from_path(paths))
    playlist.render(out)


@cli.command("loved")
@click.argument(
    "username", type=str, nargs=1, required=True, envvar="LISTENBRAINZ_USERNAME"
)
@click.option("--out", default="json", type=click.Choice(["xml", "json", "strawberry"]))
def playlist_loved_tracks(username, out: str):
    """Get the loved tracks of a user."""
    requester = PlaylistRequester()
    playlist = asyncio.run(requester.request_loved_tracks(username))
    playlist.render(out)


@cli.command("revisit-releases")
@click.argument(
    "username", type=str, nargs=1, required=True, envvar="LISTENBRAINZ_USERNAME"
)
@click.option("--count-releases", default=5, type=int)
@click.option("--out", default="json", type=click.Choice(["xml", "json", "strawberry"]))
def playlist_revisit_releases(username, count_releases: int, out: str):
    """Get releases to revisit."""
    requester = PlaylistRequester()
    playlists = asyncio.run(
        requester.request_revisit_releases(username, count_releases)
    )

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
@click.option("--count-artists", default=5, type=int)
@common_options
def playlist_suggested_artists(
    username: str, count_artists: int, out: str, n: int, seed: int, shuffle: bool
):
    """Get playlists of suggested artists."""
    requester = PlaylistRequester(tracks=n, seed=seed, shuffle=shuffle)
    playlists = asyncio.run(
        requester.request_user_artist_suggestions(username, count_artists)
    )

    # give users a choice of playlists and wait for input
    click.echo("Choose a playlist:")
    for i, playlist in enumerate(playlists):
        click.echo(f"{i}: {playlist.description}")

    choice = click.prompt("Choice", type=int)

    playlists[choice].render(out)


if __name__ == "__main__":
    cli()
