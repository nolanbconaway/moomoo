"""A thin client for moomoo.

Eventually this should be split out into a separate package but for now it's
easier to keep it here.
"""
import json
import os
from pathlib import Path

import click
import requests

from .utils_ import PlaylistResult


@click.group()
def cli():
    """Cli group for moomoo client."""
    pass


@cli.group()
def playlist():
    """Playlist commands."""
    pass


@playlist.command("from-path")
@click.argument(
    "paths", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True
)
@click.option("--n", default=20, type=int)
@click.option("--seed", default=1, type=int)
@click.option("--shuffle", default=True, type=bool)
@click.option("--out", default="json", type=click.Choice(["xml", "json", "strawberry"]))
def from_path(paths: list[Path], n: int, seed: int, shuffle: bool, out: str):
    """Get a playlist from a path."""
    host = os.environ["MOOMOO_HOST"]
    media_library = Path(os.environ["MOOMOO_MEDIA_LIBRARY"])

    if not media_library.exists():
        raise ValueError(f"Media library {media_library} does not exist.")

    if any(p == media_library for p in paths):
        raise ValueError("Media library cannot be used as a source path.")

    # ensures paths are relative to media library
    args = [("path", Path(p).resolve().relative_to(media_library)) for p in paths]

    # add other args
    args += [("n", n), ("seed", seed), ("shuffle", shuffle)]

    # from parent path allows files or folders but only one path.
    # from files only allows files but multiple paths.
    if len(paths) == 1:
        endpoint = "from-parent-path"
    else:
        if not all(p.is_file() for p in paths):
            raise ValueError(
                "Multiple paths must be files. "
                + "Otherwise a single parent path should be provided."
            )
        endpoint = "from-files"

    resp = requests.get(f"{host}/playlist/{endpoint}", params=args)

    if resp.status_code != 200:
        try:
            data = resp.json()
            data["status_code"] = resp.status_code
            click.echo(json.dumps(data))
        finally:
            resp.raise_for_status()

    if not resp.json()["success"]:
        raise RuntimeError(resp.json()["error"])

    result = PlaylistResult(
        playlist=[media_library / f for f in resp.json()["playlist"]],
        source_paths=[media_library / f for f in resp.json()["source_paths"]],
    )

    # TODO: any check that the files exist?
    result.render(out)


if __name__ == "__main__":
    cli()
