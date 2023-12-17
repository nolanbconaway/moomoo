"""Playlist generation utilities for mbids."""

import os
import random
from pathlib import Path
from uuid import UUID

from sqlalchemy.orm import Session

from ..db import execute_sql_fetchall
from .base import BasePlaylistGenerator, NoFilesRequestedError, get_most_similar_tracks


class FromMbidsPlaylistGenerator(BasePlaylistGenerator):
    """Generate playlists based on a list of mbids provided by the user.

    Automatically resolves the mbid types, and includes all files for the mbids in cases
    where the mbid is a parent (e.g. release group).
    """

    name = "from-mbids"
    limit_source_paths = 25

    def __init__(self, *mbids: UUID):
        if not mbids:
            raise ValueError("At least one mbid must be provided.")

        # dedupe
        self.mbids = list(set([mbid for mbid in mbids]))

    @classmethod
    def _files_for_recording_mbids(
        cls, mbids: list[UUID], session: Session
    ) -> list[Path]:
        """Get all files for a list of recording mbids."""
        if not mbids:
            return []

        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        sql = f"""
            select distinct filepath
            from {schema}.file_recording_map
            where recording_mbid = any(:mbids)
        """
        res = [
            Path(r["filepath"])
            for r in execute_sql_fetchall(session, sql, params=dict(mbids=mbids))
        ]
        return sorted(res)

    @classmethod
    def _files_for_release_mbids(
        cls, mbids: list[UUID], session: Session
    ) -> list[Path]:
        """Get all files for a list of release mbids.

        Does so by listing all the recording mbids, then getting those files.
        """
        if not mbids:
            return []

        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        sql = f"""
            select distinct recording_mbid
            from {schema}.recording_release_long
            where release_mbid = any(:mbids)
        """
        recording_mbids = [
            r["recording_mbid"]
            for r in execute_sql_fetchall(session, sql, params=dict(mbids=mbids))
        ]
        return cls._files_for_recording_mbids(recording_mbids, session)

    @classmethod
    def _files_for_release_group_mbids(
        cls, mbids: list[UUID], session: Session
    ) -> list[Path]:
        """Get all files for a list of release group mbids.

        Does so by listing all the recording mbids, then getting those files.
        """
        if not mbids:
            return []

        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        sql = f"""
            select distinct recording_mbid
            from {schema}.recording_release_long
            where release_group_mbid = any(:mbids)
        """
        recording_mbids = [
            r["recording_mbid"]
            for r in execute_sql_fetchall(session, sql, params=dict(mbids=mbids))
        ]
        return cls._files_for_recording_mbids(recording_mbids, session)

    @classmethod
    def _files_for_artist_mbids(cls, mbids: list[UUID], session: Session) -> list[Path]:
        """Get all files for a list of release group mbids.

        Does so by listing all the release group mbids, then getting the files for those
        release groups.
        """
        if not mbids:
            return []

        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        sql = f"""
            select distinct release_group_mbid
            from {schema}.release_artists_long
            where artist_mbid = any(:mbids)
        """
        release_group_mbids = [
            r["release_group_mbid"]
            for r in execute_sql_fetchall(session, sql, params=dict(mbids=mbids))
        ]
        return cls._files_for_release_group_mbids(release_group_mbids, session)

    def list_source_paths(self, session: Session) -> list[Path]:
        """Fetch the local files for the mbids.

        Returns a list of files, which may be empty. It should not be considered sorted.

        If over the limit, a random sample will be returned.
        """
        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        sql = f"select mbid, entity from {schema}.mbids where mbid = any(:mbids)"
        entity_types = {
            r["mbid"]: r["entity"]
            for r in execute_sql_fetchall(
                session=session, sql=sql, params={"mbids": self.mbids}
            )
        }

        def getter(entity_type: str) -> list[UUID]:
            return [k for k, v in entity_types.items() if v == entity_type]

        # start listing out potential files
        files = (
            self._files_for_recording_mbids(getter("recording"), session)
            + self._files_for_release_mbids(getter("release"), session)
            + self._files_for_release_group_mbids(getter("release-group"), session)
            + self._files_for_artist_mbids(getter("artist"), session)
        )

        # dedupe and limit
        files = list(set(files))
        if len(files) > self.limit_source_paths:
            files = random.sample(files, self.limit_source_paths)

        return files

    def get_playlist(
        self,
        session: Session,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
    ) -> tuple[list[Path], list[Path]]:
        """Get a playlist of similar songs.

        Args:
            limit: Number of songs to include in the playlist.
            shuffle: Shuffle the playlist or not.
            seed_files: Files which will be included at the start of the playlist.
            limit_per_artist: Maximum number of songs per artist.
            seed_count: Number of seed files from the request to include at the start of
                the playlist. This count is included in the limit; so if limit=10 and
                seed_count=2, 8 songs will be added to the playlist in addition to the
                seed files.
            session: Optional sqlalchemy session to use.

        Returns:
            A tuple of (playlist, source_paths).
        """
        source_paths = list(self.list_source_paths(session=session))
        if not source_paths:
            raise NoFilesRequestedError("No paths requested (or found via request).")

        seed_files = [] if seed_count == 0 else random.sample(source_paths, seed_count)
        tracks = get_most_similar_tracks(
            filepaths=source_paths,
            session=session,
            limit=limit - seed_count,
            limit_per_artist=limit_per_artist,
        )

        # reduce to just the filepaths
        tracks = [t.filepath for t in tracks]

        if shuffle:
            random.shuffle(tracks)

        return seed_files + tracks, source_paths
