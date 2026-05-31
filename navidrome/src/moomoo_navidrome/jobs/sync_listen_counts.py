"""Sync listen counts from moomoo database to navidrome.

WARNING: This is an ad-hoc script. It is not tested, not maintained, and not considered "good
software." You must shut down Navidrome before running this to avoid database corruption.
"""

import os
from pathlib import Path

import click

from moomoo_navidrome.db import execute_sql_fetchall
from moomoo_navidrome.logger import logger
from moomoo_navidrome.navidrome import NavidromeDBClient
from moomoo_navidrome.utils_ import batched


def update_play_counts(
    user_id: str,
    listen_counts: dict[Path, int],
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

    # Resolve paths to Navidrome IDs
    file_paths = list(listen_counts.keys())
    path_to_id = db_client.resolve_paths_to_ids(file_paths)

    if not path_to_id:
        logger.warning("No paths could be resolved to Navidrome IDs.")
        return 0

    # Prepare updates: filter to only resolved paths
    updates = [
        (user_id, path_to_id[path], "media_file", listen_counts[path])
        for path in file_paths
        if path in path_to_id
    ]

    if not updates:
        logger.warning("No updates to apply after path resolution.")
        return 0

    logger.info(f"Applying {len(updates)} listen count updates for user {user_id}.")

    with db_client.connect() as conn:
        cursor = conn.cursor()
        updated_count = 0
        for chunk in batched(updates, chunk_size):
            cursor.executemany(
                "insert or replace into annotation "
                "(user_id, item_id, item_type, play_count, play_date) "
                "values (?, ?, ?, ?, datetime('now'))",
                chunk,
            )
            updated_count += cursor.rowcount
        conn.commit()
    return updated_count


def fetch_listen_counts() -> dict[Path, int]:
    """Fetch listen counts from moomoo database.

    TODO: Implement this with your postgres query.
    Should return a mapping of file paths to listen counts.

    Returns:
        dict[Path, int]: Mapping of file paths to play counts.
    """

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select filepath, round(lifetime_listen_count)::int as lifetime_listen_count
        from {schema}.file_listen_counts
        where lifetime_listen_count > 1
    """
    return {
        Path(row["filepath"]): row["lifetime_listen_count"] for row in execute_sql_fetchall(sql)
    }


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
    logger.info("Fetching listen counts from moomoo database...")
    listen_counts = fetch_listen_counts()

    if not listen_counts:
        logger.warning("No listen counts found.")
        return

    logger.info(f"Found {len(listen_counts)} tracks with listen counts.")

    # ask the user for confirmation before proceeding
    if not click.confirm(
        "This will update the Navidrome database with the fetched listen counts and eliminate last "
        "played dates. Do you want to continue?"
    ):
        logger.info("Aborting sync.")
        return

    db_client = NavidromeDBClient(readonly=False)
    updated = update_play_counts(user_id, listen_counts, db_client)
    logger.info(f"Successfully updated {updated} records.")


if __name__ == "__main__":
    sync()
