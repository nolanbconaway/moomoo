"""Test the collect_msid_map module."""

import datetime
import uuid

import pytest
from click.testing import CliRunner
from sqlalchemy import text

from moomoo_ingest import collect_msid_map
from moomoo_ingest.db import LocalFile, MessyBrainzNameMap, get_session


def load_local_files_table(data: list[dict]):
    """Locad the local files table with data.

    The input rows are dicts with keys:

        - recording_md5: str
        - recording_name: str
        - artist_name: str
    """
    LocalFile.create()
    with get_session() as session:
        sql = f"""
        insert into {LocalFile.table_name()} 
            (
                filepath
                , recording_md5
                , recording_name
                , release_name
                , artist_name
                , json_data
                , file_created_at
                , file_modified_at
            )
        values
            (
                :filepath
                , :recording_md5
                , :recording_name
                , :release_name
                , :artist_name
                , :json_data
                , :file_created_at
                , :file_modified_at
            )
        """
        for row in data:
            session.execute(
                text(sql),
                params={
                    **row,
                    "json_data": '{"a":1}',
                    "filepath": str(uuid.uuid4()),
                    "file_created_at": datetime.datetime.now(),
                    "file_modified_at": datetime.datetime.now(),
                },
            )
        session.commit()


@pytest.fixture(autouse=True)
def monkeypatch_lb_get(monkeypatch):
    """Auto mock the ListenBrainz._get method."""
    monkeypatch.setattr(
        "liblistenbrainz.ListenBrainz._get", lambda *_, **__: dict(recording_name="ok")
    )


@pytest.fixture
def fake_recordings() -> list[dict]:
    """Some fake recordings to work with."""
    return [
        dict(
            recording_md5=str(uuid.uuid4()),
            recording_name="ok",
            artist_name="ok",
            release_name="ok",
        )
        for _ in range(10)
    ]


@pytest.mark.parametrize(
    "args, exit_0",
    [
        ([], True),
        (["--before=2020-01-01"], True),
        (["--before=2020-01-01", "--new"], True),
        (["--limit=0"], False),  # limit < 1
    ],
)
def test_cli_date_args(args, exit_0):
    """Test the datetime flags are required together."""
    MessyBrainzNameMap.create()
    load_local_files_table([])  # empty data, should do nothing
    runner = CliRunner()

    # no args, good to go.
    result = runner.invoke(collect_msid_map.main, args)
    if exit_0:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


def test_get_new_recordings__no_data():
    """Test the new getter when the table is empty."""
    MessyBrainzNameMap().create()
    load_local_files_table([])
    res = collect_msid_map.get_new_recordings()
    assert res == []


def test_get_new_recordings__any_data(fake_recordings: list[dict]):
    """Test the new getter when the table is not empty."""
    MessyBrainzNameMap.create()
    load_local_files_table(fake_recordings)
    res = collect_msid_map.get_new_recordings()
    assert len(res) == len(fake_recordings)


def test_get_old_recordings__no_data():
    """Test the old getter when the table is empty."""
    MessyBrainzNameMap.create()
    load_local_files_table([])
    res = collect_msid_map.get_old_recordings(before=datetime.datetime.now())
    assert res == []


def test_get_old_recordings__any_data(fake_recordings: list[dict]):
    """Test the old getter when the table is not empty."""
    MessyBrainzNameMap.create()
    load_local_files_table(fake_recordings)

    ts = datetime.datetime(2022, 1, 1)

    # add some recordings
    for i in fake_recordings:
        MessyBrainzNameMap(**i, success=True, payload_json=dict(a=1), ts_utc=ts).insert()

    # all recordings are older than the target before
    res = collect_msid_map.get_old_recordings(before=datetime.datetime.now())
    assert len(res) == len(fake_recordings)

    # skip if recordings are more recent
    res = collect_msid_map.get_old_recordings(before=ts - datetime.timedelta(days=1))
    assert len(res) == 0


def test_cli_main__no_recordings():
    """Test nothing is done if nothing is requested."""
    MessyBrainzNameMap.create()
    load_local_files_table([])  # empty data, should do nothing
    runner = CliRunner()

    # nothing to do
    result = runner.invoke(collect_msid_map.main)
    assert "Found 0 total recording(s) to ingest." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0

    # nothing is done if no new recordings are found.
    result = runner.invoke(collect_msid_map.main, ["--new"])
    assert "Found 0 total recording(s) to ingest." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0

    # nothing is done if no re-ingest recordings are found.
    result = runner.invoke(collect_msid_map.main, ["--before=2023-01-01"])
    assert "Found 0 total recording(s) to ingest." in result.output
    assert "Nothing to do." in result.output
    assert result.exit_code == 0


def test_cli_main__not_table_exists_error(fake_recordings: list[dict]):
    """Test handling of the target table not existing."""
    load_local_files_table(fake_recordings)  # table now exists with data
    runner = CliRunner()
    result = runner.invoke(collect_msid_map.main, ["--new"])
    assert result.exit_code != 0
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


def test_cli_main__new(fake_recordings: list[dict]):
    """Test working with new recordings."""
    # add the mbids to the list but without annotations
    MessyBrainzNameMap.create()
    load_local_files_table(fake_recordings)

    runner = CliRunner()
    result = runner.invoke(collect_msid_map.main, ["--new"])
    assert f"Found {len(fake_recordings)} new recording(s)." in result.output
    assert result.exit_code == 0

    res = MessyBrainzNameMap.select_star()
    assert len(res) == len(fake_recordings)


def test_cli_main__old(fake_recordings: list[dict]):
    """Test working with old recordings."""
    MessyBrainzNameMap.create()
    load_local_files_table(fake_recordings)

    # add annotations for before 2021-01-01
    for i in fake_recordings:
        MessyBrainzNameMap(**i, success=True, payload_json=dict(a=1), ts_utc="2020-01-01").insert()

    runner = CliRunner()
    result = runner.invoke(collect_msid_map.main, ["--before=2021-01-01"])
    assert f"Found {len(fake_recordings)} old recording(s)." in result.output
    assert result.exit_code == 0

    res = MessyBrainzNameMap.select_star()
    assert len(res) == len(fake_recordings)


def test_cli_main__limit(fake_recordings: list[dict]):
    """Test limit handler"""
    MessyBrainzNameMap.create()
    load_local_files_table(fake_recordings)

    n = len(fake_recordings)

    limit = n // 2
    runner = CliRunner()
    result = runner.invoke(collect_msid_map.main, ["--new", f"--limit={limit}"])
    assert f"Found {n} total recording(s) to ingest." in result.output
    assert f"Limiting to {limit} recording(s) randomly." in result.output
    assert result.exit_code == 0

    res = MessyBrainzNameMap.select_star()
    assert len(res) == limit

    # drop the annotations so i can run it again
    MessyBrainzNameMap.create(drop=True)

    # limit > mbids
    limit = n * 2
    result = runner.invoke(collect_msid_map.main, ["--new", f"--limit={limit}"])
    assert f"Found {n} total recording(s) to ingest." in result.output
    assert f"Limiting to {limit} recording(s) randomly." not in result.output
    assert result.exit_code == 0

    res = MessyBrainzNameMap.select_star()
    assert len(res) == n
