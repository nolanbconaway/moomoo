"""Handlers for making HTTP requests."""
import json
import os
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Optional, Union

import click
import requests

from .utils_ import MediaLibrary, Playlist


@dataclass
class PlaylistRequester:
    """A playlist requester for http."""

    tracks: int = 20
    seed: int = 1
    shuffle: bool = True

    @cached_property
    def library(self) -> MediaLibrary:
        """Get the media library."""
        return MediaLibrary()

    @cached_property
    def host(self) -> str:
        """Get the current moomoo host."""
        host = os.environ.get("MOOMOO_HOST")
        if host is None:
            raise ValueError("MOOMOO_HOST environment variable not set.")
        return host

    def request_tuples(self) -> list[tuple[str, Union[int, bool]]]:
        """Convert to tuples, appropriate for passing to requests."""
        return [("n", self.tracks), ("seed", self.seed), ("shuffle", self.shuffle)]

    def make_request(
        self, endpoint: str, params: Optional[list[tuple[str, Union[int, bool]]]] = None
    ) -> dict:
        """Make a request to the moomoo server, handling errors."""
        resp = requests.get(
            f"{self.host}{endpoint}", params=(params or []) + self.request_tuples()
        )

        if resp.status_code != 200:
            try:
                data = resp.json()
                data["status_code"] = resp.status_code
                click.echo(json.dumps(data))
            finally:
                resp.raise_for_status()

        if not resp.json()["success"]:
            raise RuntimeError(resp.json()["error"])

        return resp.json()

    def request_playlist_from_path(self, paths: list[Path]) -> Playlist:
        """Get a playlist from one or more paths."""
        if any(p == self.library.location for p in paths):
            raise ValueError("Media library cannot be used as a source path.")

        # from parent path allows files or folders but only one path.
        # from files only allows files but multiple paths.
        if len(paths) > 1 and not all(p.is_file() for p in paths):
            raise ValueError(
                "Multiple paths must be files. "
                + "Otherwise a single parent path should be provided."
            )

        # ensures paths are relative to media library
        args = [("path", self.library.make_relative(p)) for p in paths]
        endpoint = "/playlist/from-files"

        data = self.make_request(endpoint, args)

        # expect only one playlist if success
        plist = data["playlists"][0]
        return Playlist(
            playlist=[self.library.make_absolute(f) for f in plist["playlist"]],
            description=plist["description"],
        )

    def request_user_artist_suggestions(
        self, username: str, count_artists: int
    ) -> list[Playlist]:
        """Get the suggested artist playlists for a user."""

        endpoint = f"/playlist/suggest/by-artist/{username}"
        args = [("numPlaylists", count_artists)]
        data = self.make_request(endpoint, args)

        # expect more than one playlist if success
        return [
            Playlist(
                playlist=[self.library.make_absolute(f) for f in plist["playlist"]],
                description=plist["description"],
            )
            for plist in data["playlists"]
        ]
