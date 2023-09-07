"""A thin client for moomoo.

Eventually this should be split out into a separate package but for now it's
easier to keep it here.
"""
# TODO: TEST THIS

import os
import subprocess
import tempfile
import time
from pathlib import Path

import click
import requests
import xspf_lib as xspf

HOST = os.environ["MOOMOO_HOST"]
MEDIA_LIBRARY = Path(os.environ["MOOMOO_MEDIA_LIBRARY"])


if not MEDIA_LIBRARY.exists():
    raise ValueError(f"Media library {MEDIA_LIBRARY} does not exist.")


def render_playlist(files: list[Path], out: str, **plist_kw) -> None:
    """Render an xspf playlist to stdout or a file/program.

    Supported output formats are:

        - stdout: print xspf xml string
        - strawberry: load directly into the strawberry player
    """
    playlist = xspf.Playlist(
        trackList=[xspf.Track(location=str(p)) for p in files], **plist_kw
    )

    if out == "stdout":
        click.echo(playlist.xml_string())

    elif out == "strawberry":
        with tempfile.NamedTemporaryFile() as f:
            fp = Path(f.name)
            fp.write_text(playlist.xml_string())
            subprocess.run(["strawberry", "--load", f.name])
            time.sleep(0.5)
    else:
        raise ValueError(f"Unknown output format {out}")


@click.group()
def cli():
    """Cli group for moomoo client."""
    pass


@cli.command("playlist-from-path")
@click.argument("paths", type=click.Path(exists=True), nargs=-1, required=True)
@click.option("--n", default=20, type=int)
@click.option("--seed", default=1, type=int)
@click.option("--shuffle", default=True, type=bool)
@click.option(
    "--out", default="strawberry", type=click.Choice(["stdout", "strawberry"])
)
def from_path(paths: list[Path], n: int, seed: int, shuffle: bool, out: str):
    """Get a playlist from a path."""
    # ensures paths are relative to media library
    args = [("path", Path(p).resolve().relative_to(MEDIA_LIBRARY)) for p in paths]

    # add other args
    args += [("n", n), ("seed", seed), ("shuffle", shuffle)]

    # from parent path allows files or folders, from files only allows files
    # TODO: validate the input types?
    if len(paths) == 1:
        endpoint = "from-parent-path"
    else:
        endpoint = "from-files"

    resp = requests.get(f"{HOST}/playlist/{endpoint}", params=args)

    if resp.status_code != 200:
        try:
            click.echo(resp.json())
        finally:
            resp.raise_for_status()

    if not resp.json()["success"]:
        raise RuntimeError(resp.json()["error"])

    files = [MEDIA_LIBRARY / f for f in resp.json()["paths"]]
    render_playlist(files, out)


if __name__ == "__main__":
    cli()
