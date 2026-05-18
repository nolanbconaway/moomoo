"""Sync listenbrainz loved tracks with navidrome favorites."""

import os
from pathlib import Path

import click

from moomoo_navidrome.db import execute_sql_fetchall
from moomoo_navidrome.listenbrainz import get_listenbrainz_client
from moomoo_navidrome.logger import logger
from moomoo_navidrome.navidrome import NavidromeDBClient, NavidromeHTTPClient
from moomoo_navidrome.utils_ import batched

# global ListenBrainz client, rate limiting is handled internally
listenbrainz_client = get_listenbrainz_client()


def list_db_loves() -> set[Path]:
    """List loved tracks in the moomoo database.

    This will be a proxy for the listenbrainz loved tracks, as i do not want to hit the listenbrainz
    API every time.
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    select distinct filepath
    from {schema}.loved_tracks
    """
    return set(Path(row["filepath"]) for row in execute_sql_fetchall(sql))


def resolve_recording_mbids(filepaths: set[Path], batch_size: int = 10) -> dict[Path, str]:
    """Resolve recording MBIDs for filepaths."""
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    select filepath, recording_mbid
    from {schema}.map__file_recording
    where filepath = any(:filepaths)
    """
    results = dict()
    for batch in batched(filepaths, batch_size):
        rows = execute_sql_fetchall(sql, {"filepaths": list(map(str, batch))})
        results.update({Path(row["filepath"]): row["recording_mbid"] for row in rows})
    return results


def submit_navidrome_stars(http: NavidromeHTTPClient, db: NavidromeDBClient, filepaths: set[Path]):
    """Submit feedback to navidrome for the given filepaths."""
    song_ids = db.resolve_paths_to_ids(filepaths)
    for _, song_id in song_ids.items():
        http.get("/rest/star", params={"id": song_id})


def submit_listenbrainz_feedback(filepaths: set[Path]):
    """Submit feedback to listenbrainz for the given filepaths."""
    # https://liblistenbrainz.readthedocs.io/en/latest/api_ref.html#liblistenbrainz.client.ListenBrainz.submit_user_feedback
    # TODO: write to the db so that subsequent runs do not re-send this feedback
    for _, recording_mbid in resolve_recording_mbids(filepaths).items():
        listenbrainz_client.submit_user_feedback(feedback=1, recording_mbid=str(recording_mbid))


@click.group()
def cli():
    """Commands for syncing loves."""


@cli.command()
# options for sync direction
@click.option(
    "--direction",
    type=click.Choice(["navidrome-to-listenbrainz", "listenbrainz-to-navidrome", "both"]),
    default="both",
    help="The direction to sync loves. Defaults to both.",
)
def sync(direction: str):
    """Sync loved tracks from listenbrainz to navidrome."""
    logger.info("Starting loves sync...")
    navidrome_db = NavidromeDBClient()

    nv_loves = navidrome_db.list_loved_files()
    logger.info(f"Found {len(nv_loves)} loved tracks in navidrome.")

    lb_loves = list_db_loves()
    logger.info(f"Found {len(lb_loves)} loved tracks in listenbrainz.")

    if nv_loves == lb_loves:
        logger.info("Loved tracks are already in sync.")
        return

    if direction in ["listenbrainz-to-navidrome", "both"]:
        # star anything on navidrome that is loved on listenbrainz but not starred on navidrome
        to_star = lb_loves - nv_loves
        if not to_star:
            logger.info("No tracks to star on navidrome.")
        else:
            logger.info(f"Starring {len(to_star)} tracks on navidrome...")
        with NavidromeHTTPClient() as http:
            submit_navidrome_stars(http=http, db=navidrome_db, filepaths=to_star)

    if direction in ["navidrome-to-listenbrainz", "both"]:
        # star anything on listenbrainz that is loved on navidrome but not starred on listenbrainz
        to_star = nv_loves - lb_loves
        if not to_star:
            logger.info("No tracks to star on listenbrainz.")
        else:
            logger.info(f"Starring {len(to_star)} tracks on listenbrainz...")
            submit_listenbrainz_feedback(filepaths=to_star)
