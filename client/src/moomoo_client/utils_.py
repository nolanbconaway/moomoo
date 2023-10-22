"""Utility functions for the good of all.

Put no specialty imports beyond cli, postgres here, as the thin client needs this.
"""
import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import click
import xspf_lib as xspf


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


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
