"""Playlist generation utilities for exact files provided by the user."""
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from ..db import execute_sql_fetchall
from .base import BasePlaylistGenerator, NoFilesRequestedError, Playlist, Track


class QueryPlaylistGenerator(BasePlaylistGenerator):
    """Generate playlists using the files provided by a user SQL query.

    The provided query must select a filepath column.

    This generator will not filter results or order them whatsoever, so it is up to the
    user to provide a query that selects only the files they want in the order they
    want.
    """

    name = "exact-query"

    def __init__(
        self, sql: str, params: Optional[dict] = None, description: Optional[str] = None
    ):
        self._description = description
        self.sql = sql
        self.params = params or {}

    @property
    def description(self) -> Optional[str]:
        """Get the description for this playlist."""
        return self._description

    def fetch_filepaths(self, session: Session) -> list[Path]:
        """List the paths requested by the user that are in the database."""
        return [
            Path(row["filepath"])
            for row in execute_sql_fetchall(
                session=session, sql=self.sql, params=self.params
            )
        ]

    def get_playlist(self, session: Session, *_, **__) -> Playlist:
        """Get a playlist of similar songs.

        Args:
            session: sqlalchemy session to use.
            shuffle: Shuffle the playlist or not.

        Returns:
            A Playlist object.
        """
        paths = self.fetch_filepaths(session)
        if not paths:
            raise NoFilesRequestedError("No paths requested (or found via request).")

        res = Playlist(
            tracks=[Track(filepath=p) for p in paths], description=self.description
        )

        return res
