"""Utility functions for the good of all."""
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Optional, Union
from uuid import UUID, uuid4

import click
import xspf_lib as xspf

from .logger import logger

VERSION = (Path(__file__).resolve().parent / "version").read_text().strip()


class MediaLibrary:
    """A media library."""

    @cached_property
    def location(self) -> Path:
        library = os.environ.get("MOOMOO_MEDIA_LIBRARY")

        if library is None:
            raise ValueError("MOOMOO_MEDIA_LIBRARY environment variable not set.")

        library = Path(library)

        if not library.exists():
            raise ValueError(f"Media library {library} does not exist.")

        logger.info("location", location=library)
        return library

    def make_relative(self, path: Path) -> Path:
        """Make a path relative, within the media library.

        E.g., /home/user/music/album/track.mp3 -> album/track.mp3
        """
        return path.resolve().relative_to(self.location)

    def make_absolute(self, path: Union[Path, str]) -> Path:
        """Make a path absolute, within the media library.

        E.g., album/track.mp3 -> /home/user/music/album/track.mp3
        """
        return self.location / path


@dataclass
class Playlist:
    """A playlist.

    Contains the target paths for media, and metadata for user context. Has methods to
    render the playlist in different formats.
    """

    playlist: list[Path]
    generator: str
    description: Optional[str] = None

    # user should never set this
    playlist_id: UUID = field(default_factory=uuid4, init=False, repr=False)

    def to_xspf(self) -> xspf.Playlist:
        """Convert to an xspf playlist."""
        return xspf.Playlist(
            trackList=[xspf.Track(location=str(p)) for p in self.playlist],
            creator="moomoo",
            annotation=self.description,
        )

    def to_json(self) -> str:
        """Convert to a json string."""
        return json.dumps(
            dict(playlist=[str(p) for p in self.playlist], description=self.description)
        )

    def to_xml(self):
        """Convert to an xspf xml string."""
        return self.to_xspf().xml_string()

    def to_strawberry(self, wait_seconds: float = 0.5):
        """Load the playlist into strawberry."""
        with tempfile.NamedTemporaryFile() as f:
            fp = Path(f.name)
            fp.write_text(self.to_xml())
            subprocess.run(["strawberry", "--load", f.name])
            time.sleep(wait_seconds)

    def render(self, method: str):
        """Render the playlist."""
        if method not in ["json", "xml", "strawberry"]:
            raise ValueError(f"Unknown method {method}")

        if method == "json":
            click.echo(self.to_json())
        elif method == "xml":
            click.echo(self.to_xml())
        elif method == "strawberry":
            self.to_strawberry()
