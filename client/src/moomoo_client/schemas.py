"""Utility functions for the good of all."""

import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from dataclasses import fields as dataclass_fields
from functools import cached_property
from pathlib import Path
from uuid import UUID, uuid4

import click
import xspf_lib as xspf

from .logger import logger


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

    def make_absolute(self, path: Path | str) -> Path:
        """Make a path absolute, within the media library.

        E.g., album/track.mp3 -> /home/user/music/album/track.mp3
        """
        return self.location / path


@dataclass
class Track:
    """A track in a playlist."""

    filepath: Path
    track_length_seconds: int | None = None

    def __init__(self, **kwargs):
        """Allow extra kwargs for future proofing."""
        names = set([f.name for f in dataclass_fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)

    def to_dict(self) -> dict:
        """Convert to a dict.

        Ensure this is always serializable to json!
        """
        return {
            "filepath": str(self.filepath),
            "track_length_seconds": self.track_length_seconds,
        }


@dataclass
class Playlist:
    """A playlist.

    Contains the target paths for media, and metadata for user context. Has methods to
    render the playlist in different formats.
    """

    playlist: list[Track]
    generator: str
    description: str | None = None

    # user should never set this
    playlist_id: UUID = field(default_factory=uuid4, init=False, repr=False)

    def to_xspf(self) -> xspf.Playlist:
        """Convert to an xspf playlist."""
        return xspf.Playlist(
            trackList=[
                xspf.Track(
                    location=str(track.filepath),
                    duration=(
                        # xspf duration is in milliseconds
                        track.track_length_seconds * 1000
                        if track.track_length_seconds
                        else None
                    ),
                )
                for track in self.playlist
            ],
            creator="moomoo",
            annotation=self.description,
        )

    def to_json(self) -> str:
        """Convert to a json string."""
        return json.dumps(
            dict(
                playlist=[track.to_dict() for track in self.playlist],
                description=self.description,
            )
        )

    def to_m3u8(self) -> str:
        """Convert to an m3u8 string."""
        lines = ["#EXTM3U"]
        if self.description:
            lines.append(f"#PLAYLIST:{self.description}")
        for track in self.playlist:
            seconds = track.track_length_seconds or 0
            lines.append(f"#EXTINF:{seconds},")
            lines.append(str(track.filepath))
        return "\n".join(lines)

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
        if method not in ["json", "xml", "strawberry", "m3u8"]:
            raise ValueError(f"Unknown method {method}")

        if method == "json":
            click.echo(self.to_json())
        elif method == "xml":
            click.echo(self.to_xml())
        elif method == "strawberry":
            self.to_strawberry()
        elif method == "m3u8":
            click.echo(self.to_m3u8())
