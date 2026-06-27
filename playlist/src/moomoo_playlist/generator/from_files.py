"""Playlist generation utilities for user provided files."""

import os
import random
from pathlib import Path

from moomoo_pg import PlaylistTrack, db_retry, execute_sql_fetchall, make_temp_table
from sqlalchemy.orm import Session

from .base import (
    BasePlaylistGenerator,
    NoFilesRequestedError,
    fetch_recently_played_tracks,
    fetch_user_listen_counts,
    get_most_similar_tracks,
)


class FromFilesPlaylistGenerator(BasePlaylistGenerator):
    """Generate playlists based on a list of files provided by the user.

    Automatically handles parent vs file requests; if a parent is requested, all
    children will be included in the playlist.

    Args:
        files: Files to include in the playlist.
        username: Username for which to generate the playlist. If provided, source
            paths can be weighted based on the user'r listening history.
    """

    limit_source_paths = 100

    def __init__(self, *files: Path, username: str | None = None):
        if not files:
            raise ValueError("At least one file must be provided.")

        self.files = list(set(files))  # dedupe
        self.username = username

        if len(self.files) > self.limit_source_paths:
            self.files = random.sample(self.files, self.limit_source_paths)

    @db_retry
    def list_source_tracks(self, session: Session) -> list[PlaylistTrack.Data]:
        """List the paths requested by the user that are in the database."""
        schema = os.environ["MOOMOO_DBT_SCHEMA"]
        if len(self.files) == 1:
            # easy case, just get all files that start with the requested path
            sql = f"""
                select
                  filepath
                  , recording_mbid
                  , release_mbid
                  , release_group_mbid
                  , artist_mbid
                  , coalesce(album_artist_mbid, artist_mbid) as album_artist_mbid
                  , floor(track_length_seconds)::int as track_length_seconds
                from {schema}.local_files
                where filepath like :path
                order by random()
                limit {self.limit_source_paths}
            """
            params = {"path": f"{self.files[0]}%"}
        else:
            # filter to only the files that exactly match a known file in the db
            #
            # This invloves filtering the files to only those that are in the database.
            # The approach below is to upload the files to a temp table, and then join
            # them to the local_files table to ensure they exist.
            #
            # This avoids a potentially huge WHERE clause.
            tmp_name = make_temp_table(
                session=session,
                types={"filepath": "text"},
                data=[{"filepath": str(f)} for f in self.files],
                pk="filepath",
            )

            # join them to the local_files table to ensure they exist
            sql = f"""
                select
                  filepath
                  , recording_mbid
                  , release_mbid
                  , release_group_mbid
                  , artist_mbid
                  , coalesce(album_artist_mbid, artist_mbid) as album_artist_mbid
                from {schema}.local_files
                inner join {tmp_name} using (filepath)
            """
            params = None

        return sorted(
            [
                PlaylistTrack.Data(is_seed=True, **row)
                for row in execute_sql_fetchall(session=session, sql=sql, params=params)
            ],
            key=lambda t: t.filepath,
        )

    def get_tracks(
        self,
        session: Session,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
        recency_fac: float = 0.0,
    ) -> list[PlaylistTrack.Data]:
        """Get a list of tracks of similar songs.

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
            recency_fac: Factor by which to multiply the distance to account for recency (recently
                listened tracks are considered less similar). Default is 0.0, which means no
                recency factor.

        Returns:
            A list of PlaylistTrack.Data objects.
        """
        source_tracks = self.list_source_tracks(session)
        source_paths = [t.filepath for t in source_tracks]
        if not source_tracks:
            raise NoFilesRequestedError("No paths requested (or found via request).")

        if seed_count == 0:
            seed_tracks = []
        elif seed_count > len(source_tracks):
            seed_tracks = source_tracks
        else:
            seed_tracks = random.sample(source_tracks, seed_count)

        if self.username is not None:
            listen_counts = fetch_user_listen_counts(
                filepaths=source_paths, session=session, username=self.username
            )
            weights = [self.listen_count_to_weight(listen_counts.get(fp, 0)) for fp in source_paths]
        else:
            weights = None

        if recency_fac > 0.0:
            predicate_weights = {
                k: self.recency_score_to_weight(v, recency_fac)
                for k, v in fetch_recently_played_tracks(
                    session=session, username=self.username
                ).items()
            }
        else:
            predicate_weights = None

        tracks = get_most_similar_tracks(
            filepaths=source_paths,
            session=session,
            limit=limit - seed_count,
            limit_per_artist=limit_per_artist,
            weights=weights,
            predicate_weights=predicate_weights,
        )

        if shuffle:
            random.shuffle(tracks)

        res = seed_tracks + tracks
        return res
