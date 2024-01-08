"""Playlist generation utilities for user provided files."""

import os
import random
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from ..db import execute_sql_fetchall
from .base import (
    BasePlaylistGenerator,
    NoFilesRequestedError,
    Playlist,
    Track,
    get_most_similar_tracks,
)


class FromFilesPlaylistGenerator(BasePlaylistGenerator):
    """Generate playlists based on a list of files provided by the user.

    Automatically handles parent vs file requests; if a parent is requested, all
    children will be included in the playlist.
    """

    name = "from-files"
    limit_source_paths = 25

    def __init__(self, *files: Path, description: Optional[str] = None):
        if not files:
            raise ValueError("At least one file must be provided.")

        self._description = description
        self.files = list(set(files))  # dedupe

        if len(self.files) > self.limit_source_paths:
            self.files = random.sample(self.files, self.limit_source_paths)

    @property
    def description(self) -> Optional[str]:
        """Get the description for this playlist."""
        return self._description

    def list_source_paths(self, session: Session) -> list[Path]:
        """List the paths requested by the user that are in the database."""
        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        if len(self.files) == 1:
            sql = f"""
                select filepath
                from {schema}.local_files
                where filepath like :path
                order by random()
                limit {self.limit_source_paths}
            """
            params = {"path": f"{self.files[0]}%"}
        else:
            sql = f"""
                select filepath
                from {schema}.local_files
                where filepath = any(:filepaths)
            """
            params = {"filepaths": list(map(str, self.files))}

        return sorted(
            [
                Path(row["filepath"])
                for row in execute_sql_fetchall(session=session, sql=sql, params=params)
            ]
        )

    def get_playlist(
        self,
        session: Session,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
    ) -> Playlist:
        """Get a playlist of similar songs.

        Args:
            session: sqlalchemy session to use.
            limit: Number of songs to include in the playlist.
            shuffle: Shuffle the playlist or not.
            seed_files: Files which will be included at the start of the playlist.
            limit_per_artist: Maximum number of songs per artist.
            seed_count: Number of seed files from the request to include at the start of
                the playlist. This count is included in the limit; so if limit=10 and
                seed_count=2, 8 songs will be added to the playlist in addition to the
                seed files.

        Returns:
            A Playlist object.
        """
        source_paths = self.list_source_paths(session)
        if not source_paths:
            raise NoFilesRequestedError("No paths requested (or found via request).")

        if seed_count == 0:
            seed_tracks = []
        else:
            seed_tracks = [
                Track(filepath=p) for p in random.sample(source_paths, seed_count)
            ]

        tracks = get_most_similar_tracks(
            filepaths=source_paths,
            session=session,
            limit=limit - seed_count,
            limit_per_artist=limit_per_artist,
        )

        res = Playlist(playlist=seed_tracks + tracks)
        if shuffle:
            res.shuffle()

        return res
