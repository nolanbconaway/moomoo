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
    assert r["_args"] == dict(mbid="123", entity="recording")

    r = utils_.annotate_mbid(mbid="123", entity="release")
    assert r["_success"] is True
    assert r["data"] == dict(b=2)
    assert r["_args"] == dict(mbid="123", entity="release")

    r = utils_.annotate_mbid(mbid="123", entity="artist")
    assert r["_success"] is True
    assert r["data"] == dict(c=3)
    assert r["_args"] == dict(mbid="123", entity="artist")

    r = utils_.annotate_mbid(mbid="123", entity="release-group")
    assert r["_success"] is True
    assert r["data"] == dict(d=4)
    assert r["_args"] == dict(mbid="123", entity="release-group")

    r = utils_.annotate_mbid(mbid="123", entity="INVALID")
    assert r["_success"] is False
    assert r["error"] == "Unknown entity type: INVALID."
    assert r["_args"] == dict(mbid="123", entity="INVALID")

    monkeypatch.setattr(utils_, "_get_recording_data", mock_raise_exception)
    r = utils_.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is False
    assert r["error"] == "foo"
    assert r["_args"] == dict(mbid="123", entity="recording")


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


def test_batch():
    x = list(range(10))
    assert list(utils_.batch(x, 3)) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert list(utils_.batch(x, 4)) == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
    assert list(utils_.batch(x, 15)) == [list(range(10))]

    # edge case: empty list
    assert list(utils_.batch([], 3)) == []

    # check that type of input is preserved
    x = tuple(range(10))
    assert list(utils_.batch(x, 3)) == [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]


def test_unique_by():
    items = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 1, "val": "c"}]
    unique_items = list(utils_.unique_by(items, key=lambda x: x["id"]))
    assert unique_items == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

    unique_items = list(utils_.unique_by(items, key=lambda x: x["val"]))
    assert unique_items == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 1, "val": "c"}]


def test_topn_from_multilists():
    l1 = [{"mbid": 1}, {"mbid": 2}, {"mbid": 3}]
    l2 = [{"mbid": 3}, {"mbid": 4}, {"mbid": 5}]
    l3 = [{"mbid": 5}, {"mbid": 6}, {"mbid": 7}]
    res = utils_.topn_from_multilists([l1, l2, l3], N=5, identity_fn=lambda x: x["mbid"])
    expected_mbids = [1, 2, 3, 4, 5]
    res_mbids = sorted([i["mbid"] for i in res])
    assert res_mbids == expected_mbids

    # test with inf limit
    res = utils_.topn_from_multilists([l1, l2, l3], N=float("inf"), identity_fn=lambda x: x["mbid"])
    expected_mbids = [1, 2, 3, 4, 5, 6, 7]
    res_mbids = sorted([i["mbid"] for i in res])
    assert res_mbids == expected_mbids
