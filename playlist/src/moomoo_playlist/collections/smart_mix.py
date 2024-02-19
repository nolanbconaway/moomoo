"""Make "smart" playlists by clustering tracks based on the user listening history."""

import dataclasses
import json
import os
from collections import defaultdict
from pathlib import Path
from uuid import UUID

import click
import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.decomposition import PCA
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..db import db_retry, execute_sql_fetchall, get_session
from ..ddl import PlaylistCollection
from ..generator import FromFilesPlaylistGenerator, NoFilesRequestedError
from ..logger import get_logger

MIN_LISTENS = 2
DIMS = 300
COLLECTION_NAME = "smart-mixes"
REFRESH_INTERVAL_HOURS = 24
logger = get_logger().bind(module=__name__)


@dataclasses.dataclass
class Track:
    """A track."""

    filepath: Path
    track_name: str
    artist_name: str
    artist_mbid: UUID
    embedding: list[float]

    def __post_init__(self):
        if isinstance(self.filepath, str):
            self.filepath = Path(self.filepath)
        if isinstance(self.artist_mbid, str):
            self.artist_mbid = UUID(self.artist_mbid)
        if isinstance(self.embedding, str):
            self.embedding = json.loads(self.embedding)


@db_retry
def fetch_tracks(username: str, session: Session) -> list[Track]:
    """Fetch tracks for a user."""
    logger.info(f"Fetching tracks for {username}.")
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        with tracks as (
            select filepath
            from {schema}.file_listen_counts
            where username=:username
              and lifetime_listen_count > :min_listens

            union distinct

            select filepath
            from {schema}.loved_tracks
            where username=:username
        )

        select
            filepath
            , lf.track_name
            , coalesce(lf.album_artist_name, lf.artist_name) as artist_name
            , coalesce(lf.album_artist_mbid, lf.artist_mbid) as artist_mbid
            , lf.embedding

        from tracks
        inner join {schema}.local_files as lf using (filepath)
        
        where lf.embedding_success
          and lf.embedding_duration_seconds > 60
          and lf.artist_mbid is not null

        order by filepath
    """

    res = execute_sql_fetchall(
        sql=sql,
        params=dict(username=username, min_listens=MIN_LISTENS),
        session=session,
    )
    logger.info(f"Fetched {len(res)} tracks.")
    return [Track(**row) for row in res]


def _run_clusterer(tracks: list[Track], n_jobs: int) -> np.ndarray:
    """Run the clusterer.

    This does the actual scikit-learn part. Its split out for mocks in tests.
    """
    if len(tracks) <= DIMS:
        raise RuntimeError(
            "Not enough tracks to cluster."
            + f"Got {len(tracks)} tracks, need > {DIMS}."
        )

    embeddings = np.stack([track.embedding for track in tracks]).astype(np.float16)

    # set seed for reproducibility
    np.random.seed(5)

    pca = PCA(n_components=DIMS)
    pca.fit(embeddings)
    clusterer = HDBSCAN(
        min_cluster_size=3,
        max_cluster_size=15,
        n_jobs=n_jobs,
        cluster_selection_method="eom",
        algorithm="brute",
    )
    clusterer.fit(pca.transform(embeddings))
    return clusterer.labels_


def make_clusters(
    tracks: list[Track], n_jobs: int, max_clusters: int
) -> list[list[Path]]:
    """Make clusters, and return the resulting clustered tracks.

    The model is conducted in the following steps:

        1. PCA is used to reduce the dimensionality of the embeddings.
        2. HDBSCAN is used to cluster the embeddings.
        3. Clusters filtered to remove clusters with < 3 artists.
        4. Return the top N, ranked by most tracks in the cluster.

    """
    logger.info("Clustering tracks.")
    cluster_labels = _run_clusterer(tracks=tracks, n_jobs=n_jobs)
    logger.info(f"Clustered tracks into {len(np.unique(cluster_labels))} clusters.")

    # group tracks by cluster
    clusters = defaultdict(list)
    for track, label in zip(tracks, cluster_labels):
        clusters[label].append(track)

    # filter clusters with < 3 artists
    clusters = [
        cluster
        for label, cluster in clusters.items()
        if label != -1  # noise cluster
        and len(set(track.artist_mbid for track in cluster)) > 2
    ]
    logger.info(f"Filtered to {len(clusters)} clusters with > 2 artists.")

    if not clusters:
        raise RuntimeError("No clusters with > 2 artists.")

    # return the top N clusters by size
    return sorted(clusters, key=len, reverse=True)[:max_clusters]


@click.command("smart-mixes")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
@click.option(
    "--count",
    required=True,
    type=click.IntRange(min=1),
    help="The max number of playlists to generate (may generate fewer).",
    default=15,
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Force refresh of collection, even if not stale.",
)
@click.option(
    "--n-jobs",
    type=click.IntRange(min=1),
    help="The number of jobs to use for clustering.",
    default=15,
)
def main(username: str, count: int, force: bool, n_jobs: int):
    """Create playlists based on the top artists in the user's listening history."""
    session = get_session()
    collection = PlaylistCollection.get_collection_by_name(
        username=username,
        collection_name=COLLECTION_NAME,
        session=session,
        refresh_interval_hours=REFRESH_INTERVAL_HOURS,
    )

    if collection.is_fresh and not force:
        logger.info("Collection is not stale; skipping.")
        return

    tracks = fetch_tracks(username=username, session=session)
    clusters = make_clusters(tracks=tracks, n_jobs=n_jobs, max_clusters=count)

    logger.info(f"Generating playlists for {len(clusters)} clusters.")

    playlists = []
    for cluster in tqdm(clusters, disable=None, total=len(clusters)):
        generator = FromFilesPlaylistGenerator(
            *[track.filepath for track in cluster], username=username
        )
        try:
            playlist = generator.get_playlist(session=session)
        except NoFilesRequestedError:
            logger.exception("No files found for cluster.")
            continue

        # describe the playlist based on the first two tracks.
        # "Song like X - Y, A - B.
        t1, t2 = cluster[:2]
        description = (
            "Songs like "
            + f"{t1.track_name} - {t1.artist_name}; "
            + f"{t2.track_name} - {t2.artist_name}"
        )

        # set title based on list index, in case there was an exception
        playlist.title = f"Smart Mix {len(playlists) + 1}"
        playlist.description = description
        playlists.append(playlist)

    if len(playlists) == 0:
        logger.warning("No playlists generated.")
        return

    collection.replace_playlists(playlists=playlists, session=session, force=force)


if __name__ == "__main__":
    main()
