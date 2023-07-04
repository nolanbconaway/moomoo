"""Test utils functinos."""
import datetime
import tempfile
from pathlib import Path

import pytest

from moomoo import utils_

UTC = datetime.timezone.utc


@pytest.mark.parametrize(
    "input, expected",
    [
        ("2022-01-01", datetime.datetime(2022, 1, 1, tzinfo=UTC)),
        ("2022-01-01 12:00", datetime.datetime(2022, 1, 1, 12, tzinfo=UTC)),
        ("2022-01-01T12:00:00+01:00", datetime.datetime(2022, 1, 1, 11, tzinfo=UTC)),
    ],
)
def test_utcfromisodate(input, expected):
    assert utils_.utcfromisodate(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (0, datetime.datetime(1970, 1, 1, tzinfo=UTC)),
        (60 * 60 * 24, datetime.datetime(1970, 1, 2, tzinfo=UTC)),
    ],
)
def test_utcfromunixtime(input, expected):
    assert utils_.utcfromunixtime(input) == expected


def test_annotate_mbid(monkeypatch):
    monkeypatch.setattr(utils_, "_get_recording_data", lambda _: dict(a=1))
    monkeypatch.setattr(utils_, "_get_release_data", lambda _: dict(b=2))
    monkeypatch.setattr(utils_, "_get_artist_data", lambda _: dict(c=3))

    r = utils_.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is True
    assert r["data"] == dict(a=1)

    r = utils_.annotate_mbid(mbid="123", entity="release")
    assert r["_success"] is True
    assert r["data"] == dict(b=2)

    r = utils_.annotate_mbid(mbid="123", entity="artist")
    assert r["_success"] is True
    assert r["data"] == dict(c=3)

    r = utils_.annotate_mbid(mbid="123", entity="INVALID")
    assert r["_success"] is False
    assert r["error"] == "Unknown entity type: INVALID."

    def raise_exception(_):
        raise Exception("foo")

    monkeypatch.setattr(utils_, "_get_recording_data", raise_exception)
    r = utils_.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is False
    assert r["error"] == "foo"


def test_annotate_mbid_batch(monkeypatch):
    monkeypatch.setattr(utils_, "_get_recording_data", lambda _: dict(a=1))
    monkeypatch.setattr(utils_, "_get_release_data", lambda _: dict(b=2))
    monkeypatch.setattr(utils_, "_get_artist_data", lambda _: dict(c=3))

    maps = [
        dict(mbid="123", entity="recording"),
        dict(mbid="456", entity="release"),
        dict(mbid="789", entity="artist"),
        dict(mbid="000", entity="INVALID"),
    ]

    results = list(utils_.annotate_mbid_batch(maps))
    assert len(results) == 4
    assert results[0]["_success"] is True
    assert results[0]["data"] == dict(a=1)
    assert results[1]["_success"] is True
    assert results[1]["data"] == dict(b=2)
    assert results[2]["_success"] is True
    assert results[2]["data"] == dict(c=3)
    assert results[3]["_success"] is False
    assert results[3]["error"] == "Unknown entity type: INVALID."


def test_resolve_db_path__file_not_found():
    """Test that errors are raised when the db file is not found."""
    schema = "test"
    with utils_.pg_connect() as conn:
        cur = conn.cursor()
        sql = f"create table {schema}.local_files_flat (filepath text primary key)"
        cur.execute(sql)

    # file does not exist
    with pytest.raises(ValueError, match="Could not find file/folder"):
        utils_.resolve_db_path(Path("fake"), schema=schema)

    # folder has no files
    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path(tmpdir)
        with pytest.raises(ValueError, match=f"Could not find any files in {p}"):
            utils_.resolve_db_path(p, schema=schema)

    # no local paths in db
    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path(tmpdir)
        (p / "foo").touch()
        with pytest.raises(
            ValueError, match=f"Could not find any matches to {p} in database."
        ):
            utils_.resolve_db_path(p, schema=schema)

        with pytest.raises(
            ValueError, match=f"Could not find any matches to {p}/foo in database."
        ):
            utils_.resolve_db_path(p / "foo", schema=schema)


def test_resolve_db_path__matching():
    schema = "test"

    with utils_.pg_connect() as conn:
        cur = conn.cursor()
        sql = f"create table {schema}.local_files_flat (filepath text primary key)"
        cur.execute(sql)
        cur.execute(f"insert into {schema}.local_files_flat values ('foo/bar/a')")
        cur.execute(f"insert into {schema}.local_files_flat values ('foo/baz/b')")
        cur.execute(f"insert into {schema}.local_files_flat values ('foo/bat/c')")

    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)
        for f in ["foo/bar/a", "foo/baz/b", "foo/bat/c", "foo/bop/d"]:
            (base_path / f).parent.mkdir(parents=True, exist_ok=True)
            (base_path / f).touch()

        # test file match works
        b, ps = utils_.resolve_db_path(base_path / "foo/bar/a", schema=schema)
        assert b == base_path
        assert ps == [Path("foo/bar/a")]

        # first level dir match works
        b, ps = utils_.resolve_db_path(base_path / "foo", schema=schema)
        assert b == base_path
        assert len(ps) == 3

        # second level dir match works
        b, ps = utils_.resolve_db_path(base_path / "foo/bar", schema=schema)
        assert b == base_path
        assert ps == [Path("foo/bar/a")]
