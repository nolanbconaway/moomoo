"""Handlers for making HTTP requests."""

import json
import os
from functools import cached_property
from pathlib import Path
from typing import Optional, Union

import click
import httpx

from .logger import logger
from .utils_ import MediaLibrary, Playlist

HTTP_TIMEOUT = float(os.environ.get("MOOMOO_HTTP_TIMEOUT", "60.0"))


class PlaylistRequester:
    """A playlist requester for http."""

    def __init__(self, **kw) -> None:
        """Initialize the playlist requester."""
        self.args = kw

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
        return [(k, v) for k, v in self.args.items()]

    async def make_request(
        self, endpoint: str, params: Optional[list[tuple[str, Union[int, bool]]]] = None
    ) -> dict:
        """Make an async request to the moomoo server, handling errors."""
        logger.info("make_request", endpoint=endpoint, params=params)
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.host}{endpoint}",
                params=(params or []) + self.request_tuples(),
                timeout=HTTP_TIMEOUT,
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

    async def request_playlist_from_path(self, paths: list[Path]) -> Playlist:
        """Asynchronously request a playlist from a path."""
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

        data = await self.make_request(endpoint, args)

        # expect only one playlist if success
        plist = data["playlists"][0]
        return Playlist(
            playlist=[
                self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
            ],
            description=plist.get("description"),
            generator="from-path",
        )

    async def request_loved_tracks(self, username: str) -> Playlist:
        """Asynchronously request a playlist of loved tracks."""
        endpoint = f"/playlist/loved/{username}"
        data = await self.make_request(endpoint)

        # expect only one playlist if success
        plist = data["playlists"][0]
        return Playlist(
            playlist=[
                self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
            ],
            description=plist.get("description"),
            generator="loved",
        )

    async def request_revisit_releases(self, username: str) -> list[Playlist]:
        """Asynchronously request playlists of revisit releases."""
        endpoint = f"/playlist/revisit-releases/{username}"
        data = await self.make_request(endpoint)

        # expect more than one playlist if success
        return [
            Playlist(
                playlist=[
                    self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
                ],
                description=plist.get("description"),
                generator="revisit-releases",
            )
            for plist in data["playlists"]
        ]

    async def request_revisit_tracks(self, username: str) -> list[Playlist]:
        """Asynchronously request playlists of revisit releases."""
        endpoint = f"/playlist/revisit-tracks/{username}"
        data = await self.make_request(endpoint)

        # expect only one playlist if success
        plist = data["playlists"][0]
        return Playlist(
            playlist=[
                self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
            ],
            description=plist.get("description"),
            generator="revisit-tracks",
        )

    async def request_user_artist_suggestions(self, username: str) -> list[Playlist]:
        """Asynchronously request user artist playlist suggestions."""
        endpoint = f"/playlist/suggest/by-artist/{username}"
        data = await self.make_request(endpoint)

        # expect more than one playlist if success
        return [
            Playlist(
                playlist=[
                    self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
                ],
                description=plist.get("description"),
                generator="suggest-by-artist",
            )
            for plist in data["playlists"]
        ]

    async def request_user_smart_mixes(self, username: str) -> list[Playlist]:
        """Asynchronously request user artist playlist suggestions."""
        endpoint = f"/playlist/suggest/smart-mix/{username}"
        data = await self.make_request(endpoint)

        # expect more than one playlist if success
        return [
            Playlist(
                playlist=[
                    self.library.make_absolute(f["filepath"]) for f in plist["playlist"]
                ],
                description=plist.get("description"),
                generator="smart-mix",
            )
            for plist in data["playlists"]
        ]
