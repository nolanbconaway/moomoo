"""Utility functions for the good of all.

Put no specialty imports beyond cli, postgres here, as the thin client needs this.
"""
import datetime
import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import click
import xspf_lib as xspf


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder for UUIDs."""

    def default(self, obj):
        """Encode UUIDs as hex strings."""
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY-MM-DD date string to UTC datetime."""
    dt = datetime.datetime.fromisoformat(iso_date)
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=datetime.timezone.utc)


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )


def utcnow() -> datetime.datetime:
    """Get the current UTC datetime."""
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


@dataclass
class PlaylistResult:
    """A playlist result.

    Contains the target paths and the local paths used to generate it. Has methods
    to render the playlist in different formats.
    """

    playlist: list[Path]
    source_paths: list[Path]

    def to_xspf(self) -> xspf.Playlist:
        """Convert to an xspf playlist."""
        return xspf.Playlist(
            trackList=[xspf.Track(location=str(p)) for p in self.playlist],
            creator="moomoo",
            annotation=f"Generated via {len(self.source_paths)} source path(s).",
        )

    def to_json(self) -> str:
        """Convert to a json string."""
        return json.dumps(
            dict(
                playlist=[str(p) for p in self.playlist],
                source_paths=[str(p) for p in self.source_paths],
            )
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
