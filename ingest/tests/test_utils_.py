"""Test utils functinos."""
import datetime

import pytest
from moomoo_ingest import utils_

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


def test_md5():
    """Test basic md5 hashing."""
    with pytest.raises(TypeError):
        utils_.md5(None, "")

    assert utils_.md5("foo", "bar") == "e5f9ec048d1dbe19c70f720e002f9cb1"


def test_annotate_mbid(monkeypatch):
    monkeypatch.setattr(utils_, "_get_recording_data", lambda _: dict(a=1))
    monkeypatch.setattr(utils_, "_get_release_data", lambda _: dict(b=2))
    monkeypatch.setattr(utils_, "_get_artist_data", lambda _: dict(c=3))
    monkeypatch.setattr(utils_, "_get_release_group_data", lambda _: dict(d=4))

    r = utils_.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is True
    assert r["data"] == dict(a=1)

    r = utils_.annotate_mbid(mbid="123", entity="release")
    assert r["_success"] is True
    assert r["data"] == dict(b=2)

    r = utils_.annotate_mbid(mbid="123", entity="artist")
    assert r["_success"] is True
    assert r["data"] == dict(c=3)

    r = utils_.annotate_mbid(mbid="123", entity="release-group")
    assert r["_success"] is True
    assert r["data"] == dict(d=4)

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
    monkeypatch.setattr(utils_, "_get_release_group_data", lambda _: dict(d=4))

    maps = [
        dict(mbid="123", entity="recording"),
        dict(mbid="456", entity="release"),
        dict(mbid="789", entity="artist"),
        dict(mbid="101", entity="release-group"),
        dict(mbid="000", entity="INVALID"),
    ]

    results = list(utils_.annotate_mbid_batch(maps))
    assert len(results) == 5
    assert results[0]["_success"] is True
    assert results[0]["data"] == dict(a=1)
    assert results[1]["_success"] is True
    assert results[1]["data"] == dict(b=2)
    assert results[2]["_success"] is True
    assert results[2]["data"] == dict(c=3)
    assert results[3]["_success"] is True
    assert results[3]["data"] == dict(d=4)
    assert results[4]["_success"] is False
    assert results[4]["error"] == "Unknown entity type: INVALID."
