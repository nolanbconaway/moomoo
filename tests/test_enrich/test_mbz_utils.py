from moomoo.enrich import mbz_utils


def test_annotate_mbid(monkeypatch):
    monkeypatch.setattr(mbz_utils, "_get_recording_data", lambda _: dict(a=1))
    monkeypatch.setattr(mbz_utils, "_get_release_data", lambda _: dict(b=2))
    monkeypatch.setattr(mbz_utils, "_get_artist_data", lambda _: dict(c=3))

    r = mbz_utils.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is True
    assert r["data"] == dict(a=1)

    r = mbz_utils.annotate_mbid(mbid="123", entity="release")
    assert r["_success"] is True
    assert r["data"] == dict(b=2)

    r = mbz_utils.annotate_mbid(mbid="123", entity="artist")
    assert r["_success"] is True
    assert r["data"] == dict(c=3)

    r = mbz_utils.annotate_mbid(mbid="123", entity="INVALID")
    assert r["_success"] is False
    assert r["error"] == "Unknown entity type: INVALID."

    def raise_exception(_):
        raise Exception("foo")

    monkeypatch.setattr(mbz_utils, "_get_recording_data", raise_exception)
    r = mbz_utils.annotate_mbid(mbid="123", entity="recording")
    assert r["_success"] is False
    assert r["error"] == "foo"


def test_annotate_mbid_batch(monkeypatch):
    monkeypatch.setattr(mbz_utils, "_get_recording_data", lambda _: dict(a=1))
    monkeypatch.setattr(mbz_utils, "_get_release_data", lambda _: dict(b=2))
    monkeypatch.setattr(mbz_utils, "_get_artist_data", lambda _: dict(c=3))

    maps = [
        dict(mbid="123", entity="recording"),
        dict(mbid="456", entity="release"),
        dict(mbid="789", entity="artist"),
        dict(mbid="000", entity="INVALID"),
    ]

    results = list(mbz_utils.annotate_mbid_batch(maps))
    assert len(results) == 4
    assert results[0]["_success"] is True
    assert results[0]["data"] == dict(a=1)
    assert results[1]["_success"] is True
    assert results[1]["data"] == dict(b=2)
    assert results[2]["_success"] is True
    assert results[2]["data"] == dict(c=3)
    assert results[3]["_success"] is False
    assert results[3]["error"] == "Unknown entity type: INVALID."
