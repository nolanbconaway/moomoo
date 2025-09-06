"""Compute the artist similarity matrix using the data dump records."""

import dataclasses
import os
import sys

import click
import pandas as pd
import threadpoolctl
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import bm25_weight
from scipy.sparse import csr_matrix

from .db import (
    ListenBrainzCollaborativeFilteringScore,
    ListenBrainzDataDump,
    ListenBrainzDataDumpRecord,
    execute_sql_fetchall,
    get_session,
)
from .utils_ import batch

# improved performance by limiting the number of threads used by BLAS libraries
threadpoolctl.threadpool_limits(limits=1, user_api="blas")

MIN_USERS = 4  # require at least this many users to consider an artist in the similarity matrix
MY_USER_ID = 20589  # exclude my own listens from the similarity matrix


@dataclasses.dataclass
class DataDumpResult:
    df: pd.DataFrame

    @property
    def artist_id_map(self) -> pd.Series:
        """Return a series mapping artist_mbid to artist_id."""
        return (
            self.df.artist_mbid.drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
            .reset_index()
            .set_index("artist_mbid")
            .rename(columns={"index": "artist_id"})
            .artist_id
        )

    @property
    def artist_id_vector(self) -> pd.Series:
        """Return a vector of artist ids for the sparse matrix."""
        return self.df.join(self.artist_id_map, on="artist_mbid").artist_id

    @property
    def sparse_matrix(self) -> csr_matrix:
        res = csr_matrix((self.df["listen_count"], (self.df["user_id"], self.artist_id_vector)))
        return bm25_weight(res, K1=100, B=0.8).tocsr()


def fetch_dump_aggregate() -> DataDumpResult:
    """Get the data dump records for artist similarity matrix."""
    sql = f"""
    select
        user_id
        , artist_mbid::varchar as artist_mbid
        , sum(listen_count) as listen_count
    from {ListenBrainzDataDumpRecord.table_name()} as records
    inner join {ListenBrainzDataDump.table_name()} as dump using (slug)
    where dump.date >= (current_date - interval '365 days')
      and user_id != :my_user_id
    group by 1, 2
    """
    df = pd.DataFrame(execute_sql_fetchall(sql, {"my_user_id": MY_USER_ID}))

    # filter to artists with enough listeners
    df = df.groupby("artist_mbid").filter(lambda x: len(x.user_id.unique()) >= MIN_USERS)
    return DataDumpResult(df)


def fetch_known_artists() -> list[str]:
    """Fetch the list of known artists from the mbids table."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select distinct mbids.mbid::varchar as mbid
        from {dbt_schema}.mbids
        where mbids.entity = 'artist'
    """
    return [i["mbid"] for i in execute_sql_fetchall(sql)]


def fit_model(data_dump_result: DataDumpResult) -> AlternatingLeastSquares:
    """Fit the collaborative filtering model to the data dump."""
    show_progress = not sys.stdout.isatty()
    model = AlternatingLeastSquares(factors=64, regularization=0.05, alpha=2.0, random_state=42)
    model.fit(data_dump_result.sparse_matrix, show_progress=show_progress)
    return model


def predict_scores(
    model: AlternatingLeastSquares,
    data_dump_result: DataDumpResult,
    from_artist_mbids: list[str],
    to_artist_mbids: list[str],
) -> pd.DataFrame:
    """Predict collaborative filtering similarity scores between artists.

    For each artist in `from_artist_mbids`, compute similarity scores to all artists in
    `to_artist_mbids` using the trained collaborative filtering model. The function returns
    a DataFrame with columns:

        - artist_mbid_a: the source artist MBID
        - artist_mbid_b: the target artist MBID
        - score_value: the similarity score between the two artists

    Only artist pairs where artist_mbid_a < artist_mbid_b are included (upper triangle).
    Artists not present in the data dump or with no valid scores are excluded.

    Args:
        model: Trained AlternatingLeastSquares model from the implicit library.
        data_dump_result: DataDumpResult containing the user-artist listen matrix and mappings.
        from_artist_mbids: List of artist MBIDs to compute similarities from.
        to_artist_mbids: List of artist MBIDs to compute similarities to.

    Returns:
        pd.DataFrame: DataFrame with columns [artist_mbid_a, artist_mbid_b, score_value].
    """
    all_mbid_to_id = data_dump_result.artist_id_map
    all_id_to_mbid = all_mbid_to_id.reset_index().set_index("artist_id").artist_mbid

    from_artist_ids = all_mbid_to_id.loc[from_artist_mbids].values
    similar_ids, similar_scores = model.similar_items(from_artist_ids, N=len(all_mbid_to_id))

    # ids and scores will be a XxN matrix where X is the number of input artists and N is the total
    # number of artists in the dataset.
    #
    # convert it to long format with columns artist_mbid_a, artist_mbid_b, score_rank, score_value
    long_frame = []
    for idx, artist_id_a in enumerate(from_artist_ids):
        artist_mbid_a = all_id_to_mbid.loc[artist_id_a]  # scalar
        artist_mbid_b = all_id_to_mbid.loc[similar_ids[idx, :]].values  # vector
        scores_ = similar_scores[idx, :]  # vector
        artist_frame = pd.DataFrame(
            {"artist_mbid_a": artist_mbid_a, "artist_mbid_b": artist_mbid_b, "score_value": scores_}
        )

        # filter out artists that are not in the to_artist_mbids list
        artist_frame = artist_frame.loc[lambda x: x["artist_mbid_b"].isin(to_artist_mbids)]
        if artist_frame.empty:
            continue

        long_frame.append(artist_frame)

    # clear some memory
    del similar_ids
    del similar_scores

    # upper triangular matrix only
    long_frame = pd.concat(long_frame, ignore_index=True)
    long_frame = long_frame.loc[lambda x: x["artist_mbid_a"] < x["artist_mbid_b"]]
    return long_frame[["artist_mbid_a", "artist_mbid_b", "score_value"]]


@click.command(help=__doc__)
@click.option("--update-batch-size", default=100, type=int, help="Batch size for updating to db.")
def main(update_batch_size: int) -> None:
    # fetch the data dump aggregate
    click.echo("Fetching data dump aggregate for artist similarity matrix...")
    data_dump_result = fetch_dump_aggregate()
    if data_dump_result.df.empty:
        click.echo("No data to process for artist similarity matrix.")
        return

    # fit the model
    click.echo(f"Fitting model to {data_dump_result.df.shape[0]} records...")
    model = fit_model(data_dump_result)

    click.echo("Fetching known artists from the mbids table...")
    all_artist_mbids = fetch_known_artists()

    # filter list of artists to those that are in the data dump
    all_artist_mbids = list(
        set(all_artist_mbids).intersection(set(data_dump_result.artist_id_map.index))
    )

    # predict scores
    click.echo("Predicting scores for artist similarity matrix...")

    # replace all values in ListenBrainzCollaborativeFilteringScore with the new scores
    # batch the insert to save memory
    click.echo("Updating ListenBrainzCollaborativeFilteringScore table...")
    batches = list(batch(all_artist_mbids, update_batch_size))
    with get_session() as session:
        session.query(ListenBrainzCollaborativeFilteringScore).delete()
        ListenBrainzCollaborativeFilteringScore.reset_pk(session=session, commit=False)

        for batch_num, batch_mbids in enumerate(batches, 1):
            scores_df = predict_scores(
                model=model,
                data_dump_result=data_dump_result,
                from_artist_mbids=batch_mbids,
                to_artist_mbids=all_artist_mbids,
            )

            if scores_df.empty:
                click.echo(f"Batch {batch_num}/{len(batches)}: no scores to process.")
                continue

            # add an assumed score=1.0 for all a=b pairs
            scores_df = pd.concat(
                [
                    scores_df,
                    pd.DataFrame(
                        {
                            "artist_mbid_a": batch_mbids,
                            "artist_mbid_b": batch_mbids,
                            "score_value": 1.0,
                        }
                    ),
                ]
            )

            # do not commit until all batches are processed
            ListenBrainzCollaborativeFilteringScore.bulk_insert(
                scores_df.to_dict(orient="records"), session=session, commit=False
            )

            prop = batch_num / len(batches)
            n_rows = len(scores_df)
            click.echo(
                f"Batch {batch_num}/{len(batches)}: {n_rows} processed ({prop:.2%} complete)"
            )

        session.commit()


if __name__ == "__main__":
    main()
