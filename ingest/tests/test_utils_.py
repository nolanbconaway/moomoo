"""Test utils functinos."""

import datetime

import pytest
import requests

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


def mock_get_recording_data(_):
    """Mock function for testing annotate_mbid. Defined here bc of picklable requirement."""
    return dict(a=1)


def mock_get_release_data(_):
    """Mock function for testing annotate_mbid. Defined here bc of picklable requirement."""
    return dict(b=2)


def mock_get_artist_data(_):
    """Mock function for testing annotate_mbid. Defined here bc of picklable requirement."""
    return dict(c=3)


def mock_get_release_group_data(_):
    """Mock function for testing annotate_mbid. Defined here bc of picklable requirement."""
    return dict(d=4)


def mock_raise_exception(_):
    """Mock function for testing annotate_mbid exception handling."""
    raise Exception("foo")


def test_annotate_mbid(monkeypatch):
    monkeypatch.setattr(utils_, "_get_recording_data", mock_get_recording_data)
    monkeypatch.setattr(utils_, "_get_release_data", mock_get_release_data)
    monkeypatch.setattr(utils_, "_get_artist_data", mock_get_artist_data)
    monkeypatch.setattr(utils_, "_get_release_group_data", mock_get_release_group_data)
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

    monkeypatch.setattr(utils_, "_get_recording_data", mock_raise_exception)
    r = utils_.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is False
    assert r["error"] == "foo"


def test__get_artist_data__release_browse(monkeypatch):
    """Test the handler for browsing releases in _get_artist_data."""
    release_list = [{"id": f"release-{i}"} for i in range(250)]
    data = {
        "artist": {
            "id": "artist-mbid",
            "release-count": len(release_list),
            "release-list": release_list[:25],
        }
    }
    monkeypatch.setattr(utils_.musicbrainzngs, "get_artist_by_id", lambda *_, **__: data)

    # mock browse_releases to return slices of the release list
    monkeypatch.setattr(
        utils_.musicbrainzngs,
        "browse_releases",
        lambda limit, offset, **__: {"release-list": release_list[offset : offset + limit]},
    )
    result = utils_._get_artist_data("artist-mbid")
    assert len(result["artist"]["release-list"]) == 250


def test_request_with_retry(monkeypatch):
    call_count = dict(count=0)

    class MockResponse:
        def raise_for_status(self):
            pass

        @property
        def text(self):
            return "success"

    def mock_request(*_, **__):
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise requests.exceptions.ConnectionError("Simulated connection error.")
        return MockResponse()

    monkeypatch.setattr(requests, "request", mock_request)

    response = utils_.request_with_retry("GET", "http://example.com")
    assert response.text == "success"
    assert call_count["count"] == 3
