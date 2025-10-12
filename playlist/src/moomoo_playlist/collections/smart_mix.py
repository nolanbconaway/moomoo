"""Make "smart" playlists by clustering tracks based on the user listening history."""

import dataclasses
import json
import os
from collections import defaultdict
from pathlib import Path
from uuid import UUID

import click
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import HDBSCAN
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..config import CF_BASELINE, CF_SCALAR
from ..db import db_retry, execute_sql_fetchall, get_session
from ..ddl import PlaylistCollection
from ..generator import FromFilesPlaylistGenerator, NoFilesRequestedError
from ..logger import get_logger

collection_name = "smart-mixes"
logger = get_logger().bind(module=__name__)

# ml model constants
MIN_LISTENS = 2
RECENCY_FAC = 0.5


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


def fetch_cf_similarity_matrix(tracks: list[Track], session: Session) -> pd.DataFrame:
    """Fetch the collaborative filtering similarity matrix for the given tracks.

    Returns a DataFrame where the index and columns are artist MBIDs, and the values are
    the similarity scores. The baseline value is filled in for missing values.
    """
    logger.info(f"Fetching collaborative filtering similarity matrix for {len(tracks)} tracks.")
    if not tracks:
        return pd.DataFrame()

    # get all artist mbids; keep only first instance of each mbid
    artist_mbids = list(dict.fromkeys([track.artist_mbid for track in tracks]))

    # run the query
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select artist_mbid_a, artist_mbid_b, score_value
        from {schema}.listenbrainz_collaborative_filtering_scores
        where artist_mbid_a = any(:artist_mbids)
            and artist_mbid_b = any(:artist_mbids)
        """

    rows = execute_sql_fetchall(sql=sql, params={"artist_mbids": artist_mbids}, session=session)
    if not rows:
        logger.warning("No collaborative filtering data found; using baseline only.")
        df = pd.DataFrame(index=artist_mbids, columns=artist_mbids, dtype=np.float16)
    else:
        df = pd.DataFrame(rows).pivot(
            index="artist_mbid_a", columns="artist_mbid_b", values="score_value"
        )

    # set the index and columns to be equal and ordered by artist_mbids
    # this ensures that the matrix is square and aligned
    df = df.reindex(index=artist_mbids, columns=artist_mbids)

    # set 1 at the diagonal, baseline elsewhere
    np.fill_diagonal(df.values, 1.0)
    df = df.fillna(CF_BASELINE).astype(np.float16)
    return df


def compute_track_distance_matrix(
    tracks: list[Track], cf_matrix: pd.DataFrame
) -> np.ndarray | None:
    """Compute the track distance matrix using collaborative filtering data.

    The distance between two tracks is defined as:

        dist(t1, t2) = cosine_distance(t1, t2) / exp((cf_sim(a1, a2) - baseline) * scalar)

    Returns a square numpy array of distances, or None if there are fewer than 2 tracks.
    """
    if len(tracks) < 2:
        return None

    # get cosine distance matrix between track embeddings
    embeddings = np.stack([track.embedding for track in tracks]).astype(np.float16)
    cosine_distance = pdist(embeddings, metric="cosine")
    cosine_distance = squareform(cosine_distance)

    # create a like-sized matrix of cf similarities
    artist_mbids = [track.artist_mbid for track in tracks]
    artist_sim = cf_matrix.loc[artist_mbids, artist_mbids].values

    return cosine_distance / np.exp((artist_sim - CF_BASELINE) * CF_SCALAR)


def _run_clusterer(distance_matrix: np.ndarray, n_jobs: int) -> np.ndarray:
    """Run the clusterer.

    This does the actual scikit-learn part. Its split out for mocks in tests.
    """
    if not distance_matrix.size or distance_matrix.shape[0] <= 50:
        raise RuntimeError("Not enough tracks to cluster.")

    # set seed for reproducibility
    np.random.seed(5)

    clusterer = HDBSCAN(
        min_cluster_size=3,
        max_cluster_size=15,
        n_jobs=n_jobs,
        cluster_selection_method="eom",
        metric="precomputed",
        # copy the distance matrix to avoid an issue in which hdbscan modifies it in place
        # one day this will be fixed in hdbscan.
        # see: # https://github.com/scikit-learn/scikit-learn/issues/31907
        copy=True,
    )
    clusterer.fit(distance_matrix)
    return clusterer.labels_


def make_clusters(
    tracks: list[Track], n_jobs: int, max_clusters: int, distance_matrix=np.ndarray
) -> list[list[Track]]:
    """Make clusters, and return the resulting clustered tracks.

    The distance matrix is what is actually used for clustering. It must be square and match
    the number of tracks. The tracks are only used for the return value.
    """
    logger.info("Clustering tracks.")

    # check that distance matrix is square and matches the number of tracks
    if len(distance_matrix.shape) != 2 or distance_matrix.shape[0] != distance_matrix.shape[1]:
        raise ValueError("Distance matrix must be square.")
    if distance_matrix.shape[0] != len(tracks):
        raise ValueError("Distance matrix size must match number of tracks.")

    # get cluster labels
    cluster_labels = _run_clusterer(distance_matrix=distance_matrix, n_jobs=n_jobs)
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

    if len(clusters) <= max_clusters:
        return clusters

    # compute average distance for each cluster and return the best N
    cluster_avg_distance = []
    for cluster in clusters:
        # get the sub-distance matrix for the cluster, and take the mean of the upper triangle
        idx = [tracks.index(track) for track in cluster]
        sub_distance = distance_matrix[:, idx][idx, :]
        cluster_avg_distance.append(squareform(sub_distance).mean())

    # get indecies of the tightest clusters
    cluster_idxes = np.argsort(cluster_avg_distance)[:max_clusters]
    return [clusters[i] for i in cluster_idxes]


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
        username=username, collection_name=collection_name, session=session
    )

    if collection.is_fresh and not force:
        logger.info("Collection is not stale; skipping.")
        return

    tracks = fetch_tracks(username=username, session=session)
    if len(tracks) < 50:
        logger.warning(f"Not enough tracks ({len(tracks)}) to generate smart mixes.")
        return

    # downsample to random 2000 or 3/4 of the tracks. whatever is bigger.
    # this adds some variance to more slowly changing clusters.
    if len(tracks) > 2000:
        n = max(2000, len(tracks) * 3 // 4)
        logger.info(f"Downsampling to {n} tracks for clustering.")
        idx = np.random.choice(np.arange(len(tracks)), size=n, replace=False)
        tracks = [tracks[i] for i in idx]

    # fetch the artist similarity matrix and compute the distance matrix
    cf_matrix = fetch_cf_similarity_matrix(tracks=tracks, session=session)
    distance = compute_track_distance_matrix(tracks=tracks, cf_matrix=cf_matrix)
    clusters = make_clusters(
        tracks=tracks, n_jobs=n_jobs, max_clusters=count, distance_matrix=distance
    )

    logger.info(f"Generating playlists for {len(clusters)} clusters.")

    playlists = []
    for cluster in tqdm(clusters, disable=None, total=len(clusters)):
        logger.info(f"Generating playlist for cluster with {len(cluster)} tracks.")
        generator = FromFilesPlaylistGenerator(
            *[track.filepath for track in cluster], username=username
        )
        try:
            playlist = generator.get_playlist(
                session=session, seed_count=1, recency_fac=RECENCY_FAC
            )
        except NoFilesRequestedError:
            logger.exception("No files found for cluster.")
            continue

        # describe the playlist based on the first two tracks.
        # "Song like X - Y, A - B.
        t1, t2 = cluster[:2]
        description = (
            "Songs like: "
            + f"'{t1.track_name}' ({t1.artist_name}); "
            + f"'{t2.track_name}' ({t2.artist_name})"
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
