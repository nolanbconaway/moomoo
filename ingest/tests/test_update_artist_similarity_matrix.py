"""Unit tests for the update_artist_similarity_matrix module."""

import datetime
import typing
import uuid

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner
from implicit.als import AlternatingLeastSquares
from scipy.sparse import csr_matrix

from moomoo_ingest.db import (
    ListenBrainzCollaborativeFilteringScore,
    ListenBrainzDataDump,
    ListenBrainzDataDumpRecord,
)
from moomoo_ingest.update_artist_similarity_matrix import (
    DataDumpResult,
    fetch_dump_aggregate,
    fetch_known_artists,
    fit_model,
    main,
    predict_scores,
)

from .conftest import load_mbids_table


@pytest.fixture
def db_data_dump() -> typing.Iterator[ListenBrainzDataDump]:
    """A realistic ListenBrainzDataDump and associated listen records for model testing.

    This fixture sets up a test data dump with the following properties:
      - 10 unique artists (random UUIDs)
      - 20 unique users
      - Each artist is assigned a random number of unique users (1 to 20)
      - Each (artist, user) pair gets a random listen count (1 to 50)
      - All records are linked to a single data dump (slug 'test-slug')
      - Uses a fixed random seed for reproducibility

    The resulting data is suitable for collaborative filtering and similarity matrix tests.

    Yields:
        ListenBrainzDataDump: The created data dump object (with slug 'test-slug').
    """
    ListenBrainzDataDump.create()
    ListenBrainzDataDumpRecord.create()
    data_dump = ListenBrainzDataDump(
        slug="test-slug",
        ftp_path="/path/to/test-dump",
        ftp_modify_ts=datetime.datetime.now(),
        date=datetime.date.today(),
        start_timestamp=datetime.datetime.now(),
        end_timestamp=datetime.datetime.now(),
        refreshed_at=datetime.datetime.now(),
    )
    data_dump.insert()

    # Generate random data: 10 artists, 20 users, 200 listens
    num_artists, num_users = 10, 20
    artist_mbids = [uuid.uuid4() for _ in range(num_artists)]
    user_ids = list(range(1, num_users + 1))
    random = np.random.RandomState(42)  # For reproducibility
    records = []
    for artist_mbid in artist_mbids:
        random_user_count = random.randint(1, num_users)
        for userid in random.choice(user_ids, size=random_user_count, replace=False):
            records.append(
                {
                    "slug": "test-slug",
                    "user_id": int(userid),
                    "artist_mbid": artist_mbid,
                    "listen_count": int(random.randint(1, 50)),
                }
            )

    ListenBrainzDataDumpRecord.bulk_insert(records)
    yield data_dump


@pytest.fixture
def cf_model(db_data_dump: ListenBrainzDataDump) -> typing.Iterator[AlternatingLeastSquares]:
    """Fixture that yields a trained AlternatingLeastSquares collaborative filtering model.

    This fixture uses the db_data_dump fixture to generate a realistic ListenBrainzDataDump
    and associated listen records. It then fetches the aggregated data and fits an
    AlternatingLeastSquares model using the fit_model utility. The trained model is yielded
    for use in tests that require collaborative filtering predictions or similarity matrix
    computations.

    Yields:
        AlternatingLeastSquares: A trained collaborative filtering model from the implicit library.
    """
    data = fetch_dump_aggregate()
    yield fit_model(data)


def test_DataDumpResult__artist_id_map():
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 1, 2],
            "artist_mbid": ["b", "a", "a", "c", "b"],
            "listen_count": [10, 20, 30, 40, 50],
        }
    )
    result = DataDumpResult(df)
    artist_id_map = result.artist_id_map

    # The mapping should assign unique integer ids in sorted order of mbid
    expected_index = ["a", "b", "c"]
    expected_ids = [0, 1, 2]
    assert list(artist_id_map.index) == expected_index
    assert list(artist_id_map.values) == expected_ids


def test_DataDumpResult__artist_id_vector():
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 1, 2],
            "artist_mbid": ["b", "a", "a", "c", "b"],
            "listen_count": [10, 20, 30, 40, 50],
        }
    )
    result = DataDumpResult(df)
    # artist_id_map: a=0, b=1, c=2
    # artist_mbid column: [b, a, a, c, b] => [1, 0, 0, 2, 1]
    expected_vector = [1, 0, 0, 2, 1]
    assert list(result.artist_id_vector) == expected_vector


def test_DataDumpResult__sparse_matrix():
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 1, 2],
            "artist_mbid": ["b", "a", "a", "c", "b"],
            "listen_count": [10, 20, 30, 40, 50],
        }
    )
    result = DataDumpResult(df)
    mat = result.sparse_matrix

    assert mat.shape == (4, 3)  # The shape should be (max user_id + 1, number of unique artists)
    assert isinstance(mat, csr_matrix)  # The matrix should be a csr_matrix


def test_fetch_dump_aggregate(db_data_dump: ListenBrainzDataDump):
    # just make sure the function runs without error
    result = fetch_dump_aggregate()
    assert isinstance(result, DataDumpResult)


def test_fetch_known_artists():
    mbid1 = str(uuid.uuid4())
    mbid2 = str(uuid.uuid4())
    mbid3 = str(uuid.uuid4())
    load_mbids_table(
        [
            {"mbid": mbid1, "entity": "artist"},
            {"mbid": mbid2, "entity": "artist"},
            {"mbid": mbid3, "entity": "recording"},
        ]
    )
    result = fetch_known_artists()
    assert set(result) == {mbid1, mbid2}


def test_predict_scores(cf_model):
    """Test the predict_scores function for correct output shape and content.

    Also test cases where from_artist_mbids and to_artist_mbids are subsets of all artists.
    """
    data = fetch_dump_aggregate()
    all_mbids = sorted(list(data.artist_id_map.index))

    # Case 1: all artists
    scores_df = predict_scores(
        model=cf_model,
        data_dump_result=data,
        from_artist_mbids=all_mbids,
        to_artist_mbids=all_mbids,
    )
    # Should be a DataFrame with the correct columns
    assert set(scores_df.columns) == {"artist_mbid_a", "artist_mbid_b", "score_value"}
    assert all(scores_df["artist_mbid_a"] < scores_df["artist_mbid_b"])  # All a < b
    assert not scores_df["score_value"].isnull().any()  # No NaN values in score_value
    assert len(scores_df) > 0  # There should be at least one row if there are at least 2 artists

    # Case 2: from and to are disjoint subsets
    from_subset = all_mbids[:2]
    to_subset = all_mbids[2:4]
    scores_df2 = predict_scores(
        model=cf_model,
        data_dump_result=data,
        from_artist_mbids=from_subset,
        to_artist_mbids=to_subset,
    )
    assert set(scores_df2.columns) == {"artist_mbid_a", "artist_mbid_b", "score_value"}
    assert set(scores_df2["artist_mbid_a"]).issubset(set(from_subset))
    assert set(scores_df2["artist_mbid_b"]).issubset(set(to_subset))
    assert all(scores_df2["artist_mbid_a"] < scores_df2["artist_mbid_b"])  # All a < b
    assert not scores_df2["score_value"].isnull().any()  # No NaN values in score_value
    assert len(scores_df2) == 4  # 2 from * 2 to = 4 combinations

    # Case 3: from and to overlap but are not identical
    from_subset = all_mbids[:3]
    to_subset = all_mbids[1:4]
    scores_df3 = predict_scores(
        model=cf_model,
        data_dump_result=data,
        from_artist_mbids=from_subset,
        to_artist_mbids=to_subset,
    )
    assert set(scores_df3.columns) == {"artist_mbid_a", "artist_mbid_b", "score_value"}
    assert set(scores_df3["artist_mbid_a"]).issubset(set(from_subset))
    assert set(scores_df3["artist_mbid_b"]).issubset(set(to_subset))
    assert all(scores_df3["artist_mbid_a"] < scores_df3["artist_mbid_b"])  # All a < b
    assert not scores_df3["score_value"].isnull().any()  # No NaN values in score_value
    assert len(scores_df3) == 6  # 3 from * 2 to = 6 combinations


def test_main_cli(db_data_dump):
    """Test the CLI main function end-to-end using Click's test runner.

    Populates the mbids table with the artists in the data dump, runs the CLI,
    and asserts that the ListenBrainzCollaborativeFilteringScore table is populated
    with the expected artist pairs and scores.
    """
    ListenBrainzCollaborativeFilteringScore.create()
    data = fetch_dump_aggregate()
    all_mbids = list(data.artist_id_map.index)
    load_mbids_table([{"mbid": mbid, "entity": "artist"} for mbid in all_mbids])
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert result.exit_code == 0

    rows = sorted(
        ListenBrainzCollaborativeFilteringScore.select_star(),
        key=lambda x: (x["artist_mbid_a"], x["artist_mbid_b"]),
    )
    assert len(rows) > 0
    for row in rows:
        assert str(row["artist_mbid_a"]) in all_mbids
        assert str(row["artist_mbid_b"]) in all_mbids
        assert row["artist_mbid_a"] < row["artist_mbid_b"]
        assert isinstance(row["score_value"], float)

    # running 2x should produce the same results
    result = runner.invoke(main, [])
    assert result.exit_code == 0
    rows_2 = sorted(
        ListenBrainzCollaborativeFilteringScore.select_star(),
        key=lambda x: (x["artist_mbid_a"], x["artist_mbid_b"]),
    )
    assert [row["artist_mbid_a"] for row in rows] == [row["artist_mbid_a"] for row in rows_2]
    assert [row["artist_mbid_b"] for row in rows] == [row["artist_mbid_b"] for row in rows_2]
    assert [row["score_value"] for row in rows] == [row["score_value"] for row in rows_2]
