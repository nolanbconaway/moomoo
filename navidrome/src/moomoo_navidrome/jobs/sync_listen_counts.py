"""Sync listen counts from moomoo database to navidrome.

WARNING: This is an ad-hoc script. It is not tested, not maintained, and not considered "good
software." You must shut down Navidrome before running this to avoid database corruption.
"""

import datetime
import os
import sqlite3
from itertools import batched
from pathlib import Path

import click
from moomoo_pg import execute_sql_fetchall
from pydantic import BaseModel

from ..logger import logger
from ..navidrome import NavidromeDBClient


class PlayCount(BaseModel):
    item_id: str
    item_type: str  # "media_file", "artist", "album"
    play_count: int
    play_date: datetime.datetime

    @property
    def tup(self):
        # ISO format date string, like 2026-05-31 03:44:12
        dt = self.play_date.strftime("%Y-%m-%d %H:%M:%S")
        return (self.item_id, self.item_type, self.play_count, dt)


def update_play_counts(
    user_id: str,
    listen_counts: list[PlayCount],
    db_client: NavidromeDBClient,
    chunk_size: int = 100,
) -> int:
    """Update play counts for the given user.

    WARNING: This directly modifies the Navidrome database with INSERT OR REPLACE.
    Not thoroughly tested. Ensure you have a backup before running.

    Args:
        user_id: The Navidrome user ID.
        listen_counts: Mapping of file paths to play counts.
        db_client: NavidromeDBClient instance (must be connected with write mode enabled).
        chunk_size: Batch size for updates.

    Returns:
        int: Number of records updated.
    """
    if not listen_counts:
        return 0

    logger.info(f"Applying {len(listen_counts)} listen count updates.")

    with db_client.connect() as conn:
        cursor = conn.cursor()
        updated_count = 0
        for chunk in batched(listen_counts, chunk_size):
            cursor.executemany(
                "insert or replace into annotation "
                "(user_id, item_id, item_type, play_count, play_date) "
                "values (?, ?, ?, ?, ?)",
                [(user_id, *pc.tup) for pc in chunk],
            )
            updated_count += cursor.rowcount
        conn.commit()
    return updated_count


def fetch_file_listen_counts(navidrome_db: NavidromeDBClient) -> list[PlayCount]:
    """Fetch listen counts from moomoo database.

    TODO: Implement this with your postgres query.
    Should return a mapping of file paths to listen counts.

    Returns:
        list[PlayCount]: List of PlayCount objects representing file listen counts.
    """

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        with listen_file_map as (
        select
            listens.listen_md5
            , map_.filepath
            , count(1) over (partition by listens.listen_md5) as potential_file_count

        from {schema}.listens as listens
        inner join {schema}.map__file_recording as map_ using (recording_mbid)

        where listens.recording_mbid is not null
        )

        select
            map_.filepath
            , round(sum(1::real / map_.potential_file_count))::int as listen_count
            , max(listens.listen_at_ts_utc) as last_listen
        from listen_file_map as map_
        inner join {schema}.listens using (listen_md5)
        group by map_.filepath
        having round(sum(1::real / map_.potential_file_count))::int > 0
    """
    rows = execute_sql_fetchall(sql)
    media_ids = navidrome_db.resolve_paths_to_ids([Path(row["filepath"]) for row in rows])
    return [
        PlayCount(
            item_id=media_ids[Path(row["filepath"])],
            item_type="media_file",
            play_count=row["listen_count"],
            play_date=row["last_listen"],
        )
        for row in rows
        if Path(row["filepath"]) in media_ids
    ]


def fetch_artist_listen_counts(navidrome_db: NavidromeDBClient) -> list[PlayCount]:
    """Fetch artist-level listen counts from moomoo database."""
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select
            artist_mbid
            , lifetime_listen_count as listen_count
            , current_date - listen_recency_days::int as last_listened_date
        from {schema}.artist_listen_counts
    """
    rows = execute_sql_fetchall(sql)

    def resolve_artist_id(conn: sqlite3.Connection, mbid: str) -> str | None:
        cur = conn.execute("select id from artist where mbz_artist_id = ?", (str(mbid),))
        result = cur.fetchone()
        return result["id"] if result else None

    with navidrome_db.connect() as navidrome_conn:
        artist_ids = [resolve_artist_id(navidrome_conn, row["artist_mbid"]) for row in rows]

    return [
        PlayCount(
            item_id=artist_id,
            item_type="artist",
            play_count=row["listen_count"],
            play_date=row["last_listened_date"],
        )
        for row, artist_id in zip(rows, artist_ids, strict=True)
        if artist_id is not None
    ]


def fetch_album_listen_counts(navidrome_db: NavidromeDBClient) -> list[PlayCount]:
    """Fetch artist-level listen counts from moomoo database."""
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select
            release_mbid
            , lifetime_listen_count as listen_count
            , current_date - listen_recency_days::int as last_listened_date
        from {schema}.release_listen_counts
    """
    rows = execute_sql_fetchall(sql)

    def resolve_album_id(conn: sqlite3.Connection, mbid: str) -> str | None:
        cur = conn.execute("select id from album where mbz_album_id = ?", (str(mbid),))
        result = cur.fetchone()
        return result["id"] if result else None

    with navidrome_db.connect() as conn:
        album_ids = [resolve_album_id(conn, row["release_mbid"]) for row in rows]

    return [
        PlayCount(
            item_id=album_id,
            item_type="album",
            play_count=row["listen_count"],
            play_date=row["last_listened_date"],
        )
        for row, album_id in zip(rows, album_ids, strict=True)
        if album_id is not None
    ]


@click.command()
@click.option(
    "--user-id",
    required=True,
    help="Navidrome user ID to update listen counts for.",
)
def sync(user_id: str):
    """Sync listen counts from moomoo to navidrome.

    WARNING: This modifies the Navidrome database. Ensure Navidrome is shut down.
    This is an ad-hoc script with minimal testing. Use --dry-run first to verify behavior.
    """
    # ask the user for confirmation before proceeding
    if not click.confirm(
        "This will update the Navidrome database with the fetched listen counts and eliminate last "
        "played dates. Do you want to continue?"
    ):
        logger.info("Aborting sync.")
        return

    db_client = NavidromeDBClient(readonly=False)

    logger.info("Fetching listen counts from moomoo database...")
    file_listen_counts = fetch_file_listen_counts(db_client)
    artist_listen_counts = fetch_artist_listen_counts(db_client)
    album_listen_counts = fetch_album_listen_counts(db_client)

    logger.info(
        f"Fetched {len(file_listen_counts)} file listen counts, "
        f"{len(artist_listen_counts)} artist listen counts, "
        f"and {len(album_listen_counts)} album listen counts."
    )

    if not click.confirm("Proceed with updating Navidrome database with these listen counts?"):
        logger.info("Aborting sync.")
        return

    listen_counts = file_listen_counts + artist_listen_counts + album_listen_counts
    if not listen_counts:
        logger.warning("No listen counts found.")
        return

    updated = update_play_counts(user_id, listen_counts, db_client)
    logger.info(f"Successfully updated {updated} records.")


if __name__ == "__main__":
    sync()
