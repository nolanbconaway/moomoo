"""Manage a play queue playlist on navidrome, containing all songs that have been added.

The user needs to remove the songs, this just adds them based on the last sync.
"""

import base64
import datetime
import os
import time
from pathlib import Path
from typing import Final

import click
from pydantic import BaseModel, ConfigDict

from moomoo_navidrome.db import execute_sql_fetchall
from moomoo_navidrome.logger import logger
from moomoo_navidrome.models import NavidromePlaylist
from moomoo_navidrome.navidrome import NavidromeHTTPClient
from moomoo_navidrome.utils_ import batched


class QueueSignature(BaseModel):
    """Defines the structure of the signature comment used to identify the play queue playlist."""

    model_config = ConfigDict(extra="ignore")

    ts: datetime.datetime
    synced_at: datetime.datetime
    tag: Final[str] = "#moomoo-inbox"

    @property
    def signature(self) -> str:
        """Generate the signature comment for the playlist."""
        json_data = self.model_dump_json(exclude={"tag"})
        b64 = base64.urlsafe_b64encode(json_data.encode()).decode()

        return f"{self.tag}={b64}"

    @classmethod
    def from_comment(cls, comment: str) -> "QueueSignature | None":
        if cls.tag not in comment:
            return None

        _, right = comment.split(cls.tag, 1)
        if not right.startswith("="):
            return None
        else:
            right = right[1:]

        right = right.strip().split()[0]  # in case there are other comments after the signature
        return cls.model_validate_json(base64.urlsafe_b64decode(right).decode())


class QueuePlaylist(NavidromePlaylist):
    @property
    def signature(self) -> QueueSignature | None:
        """Parse the signature from the playlist comment, if present."""
        if not self.comment:
            return None

        return QueueSignature.from_comment(self.comment)

    @classmethod
    def fetch(cls, client: NavidromeHTTPClient) -> NavidromePlaylist:
        """Fetch the playlist with the given name, or return None if it doesn't exist."""
        playlists = client.fetch_playlists()

        # convert to QueuePlaylist and filter by signature tag
        tagged = [
            ql for pl in playlists if (ql := QueuePlaylist(**pl.model_dump())).signature is not None
        ]

        if len(tagged) > 1:
            raise RuntimeError(f"Multiple playlists with tag {cls.tag} found.")
        elif len(tagged) == 0:
            raise RuntimeError(f"Playlist with tag {cls.tag} not found.")

        return tagged[0]


def get_releases_added_since(ts: datetime.datetime) -> list[Path]:
    """Get all filepaths that have been added since the last sync."""
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    select filepath
    from {schema}.local_files
    where file_created_at > :ts
    order by filepath
    """
    rows = execute_sql_fetchall(sql, {"ts": ts})
    return [Path(row["filepath"]) for row in rows]


def get_latest_ts(filepaths: list[Path], batch_size: int = 20) -> datetime.datetime | None:
    """Get the latest timestamp from a list of filepaths."""
    if not filepaths:
        return None

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    select max(file_created_at) as latest
    from {schema}.local_files
    where filepath = any(:batch)
    """

    latest = datetime.datetime.fromtimestamp(0)
    for batch in batched(filepaths, batch_size):
        rows = execute_sql_fetchall(sql, {"batch": [str(p) for p in batch]})
        batch_latest = rows[0]["latest"]
        latest = max(latest, batch_latest)

    # should never happen but just in case...
    if latest == datetime.datetime.fromtimestamp(0):
        return None

    return latest


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--ts",
    type=click.DateTime(),
    default=None,
    help="The timestamp to use for the signature. Defaults to now.",
)
def sign(ts: datetime.datetime | None):
    """Add the signature comment to the play queue playlist."""
    ts = ts or datetime.datetime.now(datetime.timezone.utc)
    comment = QueueSignature(ts=ts, synced_at=ts).signature

    with NavidromeHTTPClient() as client:
        playlists = [i for i in client.fetch_playlists() if QueueSignature.tag in (i.comment or "")]
        if not playlists:
            raise RuntimeError(f"No playlist with tag {QueueSignature.tag} found.")
        elif len(playlists) > 1:
            raise RuntimeError(f"Multiple playlists with tag {QueueSignature.tag} found.")
        playlist = playlists[0]

        client.post(
            "/rest/updatePlaylist",
            params={"playlistId": playlist.playlist_id},
            data={"comment": comment},
        )
        time.sleep(0.1)

        check_plist = client.get_playlist_by_id(playlist.playlist_id)
        if check_plist.comment != comment:
            raise RuntimeError("Comment was not added correctly.")

    logger.info(f"Playlist '{playlist.name}' signed with timestamp {ts}.")


@cli.command()
def sync():
    """Sync the play queue playlist with the songs added since the last sync."""
    with NavidromeHTTPClient() as client:
        playlist = QueuePlaylist.fetch(client)
        logger.info(f"Fetched playlist '{playlist.name}' with id {playlist.playlist_id}.")

        logger.info(f"Last sync stopped at {playlist.signature.ts}, fetching new songs since then.")
        new_songs = get_releases_added_since(playlist.signature.ts)
        logger.info(f"Found {len(new_songs)} new songs since last sync.")
        if new_songs:
            client.add_songs_to_playlist(playlist.id, new_songs)
