import datetime
from pathlib import Path
from uuid import uuid1

from moomoo_pg import (
    ListenBrainzListen,
    ListenBrainzSimilarUserActivity,
    ListenBrainzUserFeedback,
    LocalFile,
    MessyBrainzNameMap,
    get_session,
)

from moomoo_ingest.annotate_mbids import Mbid
from moomoo_ingest.musicbrainz import mbids


def test_get_listen_mbids():
    now = datetime.datetime.now(datetime.timezone.utc)
    mapped_artist = uuid1()
    additional_artists = [uuid1(), uuid1()]
    mapped_recording = uuid1()
    additional_recording = uuid1()
    mapped_release = uuid1()
    additional_release = uuid1()
    mapped_release_group = uuid1()
    additional_release_group = uuid1()

    ListenBrainzListen(
        listen_md5="listen-mapping",
        username="user",
        json_data={
            "track_metadata": {
                "mbid_mapping": {
                    "artist_mbids": [str(mapped_artist)],
                    "recording_mbid": str(mapped_recording),
                    "release_mbid": str(mapped_release),
                    "release_group_mbid": str(mapped_release_group),
                },
                "additional_info": {
                    "artist_mbids": [str(additional_artists[0])],
                    "artist_mbid": str(additional_artists[1]),
                    "release_group_mbid": str(additional_release_group),
                },
            }
        },
        listen_at_ts_utc=now,
    ).insert()
    ListenBrainzListen(
        listen_md5="listen-additional-info",
        username="user",
        json_data={
            "track_metadata": {
                "additional_info": {
                    "recording_mbid": str(additional_recording),
                    "release_mbid": str(additional_release),
                }
            }
        },
        listen_at_ts_utc=now,
    ).insert()

    with get_session() as session:
        result = {(item.entity, item.mbid) for item in mbids.get_listen_mbids(session)}

    assert result == {
        ("artist", mapped_artist),
        ("artist", additional_artists[0]),
        ("artist", additional_artists[1]),
        ("recording", mapped_recording),
        ("recording", additional_recording),
        ("release", mapped_release),
        ("release", additional_release),
        ("release-group", mapped_release_group),
        ("release-group", additional_release_group),
    }


def test_get_local_file_mbids():
    now = datetime.datetime.now(datetime.timezone.utc)
    release = uuid1()
    release_group = uuid1()
    recording = uuid1()
    artist = uuid1()
    album_artist = uuid1()

    LocalFile(
        filepath=Path("file"),
        json_data={
            "musicbrainz_albumid": str(release),
            "musicbrainz_releasegroupid": str(release_group),
            "musicbrainz_trackid": str(recording),
            "musicbrainz_artistid": str(artist),
            "musicbrainz_albumartistid": str(album_artist),
        },
        file_created_at=now,
        file_modified_at=now,
    ).insert()

    with get_session() as session:
        result = {(item.entity, item.mbid) for item in mbids.get_local_file_mbids(session)}

    assert result == {
        ("release", release),
        ("release-group", release_group),
        ("recording", recording),
        ("artist", artist),
        ("artist", album_artist),
    }


def test_get_messybrainz_mbids():
    now = datetime.datetime.now(datetime.timezone.utc)
    artist = uuid1()
    recording = uuid1()
    release = uuid1()
    MessyBrainzNameMap(
        recording_md5="whatever",
        recording_name="whatever",
        artist_name="whatever",
        release_name="whatever",
        success=True,
        payload_json={
            "artist_mbids": [str(artist)],
            "recording_mbid": str(recording),
            "release_mbid": str(release),
        },
        ts_utc=now,
    ).insert()

    with get_session() as session:
        result = {(item.entity, item.mbid) for item in mbids.get_messybrainz_mbids(session)}

    assert result == {
        ("artist", artist),
        ("recording", recording),
        ("release", release),
    }


def test_get_similar_user_activity_mbids():
    values = {
        "artists": ("artist_mbid", "artist", uuid1()),
        "releases": ("release_mbid", "release", uuid1()),
        "recordings": (
            "recording_mbid",
            "recording",
            uuid1(),
        ),
    }
    for source_entity, (key, _, identifier) in values.items():
        ListenBrainzSimilarUserActivity(
            payload_id=source_entity,
            from_username="user",
            to_username="similar",
            entity=source_entity,
            time_range="all_time",
            user_similarity=0.5,
            json_data={source_entity: [{key: str(identifier)}]},
        ).insert()

    with get_session() as session:
        result = {
            (item.entity, item.mbid) for item in mbids.get_similar_user_activity_mbids(session)
        }

    assert result == {
        (mbid_entity, identifier) for _, (_, mbid_entity, identifier) in values.items()
    }


def test_get_feedback_mbids():
    recording = uuid1()
    ListenBrainzUserFeedback(
        feedback_md5="feedback",
        username="user",
        score=1,
        recording_mbid=recording,
        feedback_at=datetime.datetime.now(datetime.timezone.utc),
    ).insert()

    with get_session() as session:
        result = mbids.get_feedback_mbids(session)

    assert result == [Mbid(mbid=recording, entity="recording")]


def test_get_mbids_combines_deduplicates(monkeypatch):
    shared = Mbid(mbid=uuid1(), entity="artist")
    recording = Mbid(mbid=uuid1(), entity="recording")
    release = Mbid(mbid=uuid1(), entity="release")
    release_group = Mbid(mbid=uuid1(), entity="release-group")
    source_results = {
        "get_listen_mbids": [shared, recording],
        "get_local_file_mbids": [shared, release],
        "get_messybrainz_mbids": [],
        "get_similar_user_activity_mbids": [recording, release_group],
        "get_feedback_mbids": [],
    }
    for name, result in source_results.items():
        monkeypatch.setattr(mbids, name, lambda _, result=result: result)

    with get_session() as session:
        result = mbids.get_mbids(session)

    assert result == sorted(
        {shared, recording, release, release_group},
        key=lambda item: (item.entity, str(item.mbid)),
    )


def test_mbid_collection_sorts_and_groups_by_entity():
    first_artist = Mbid(mbid=uuid1(), entity="artist")
    second_artist = Mbid(mbid=uuid1(), entity="artist")
    recording = Mbid(mbid=uuid1(), entity="recording")

    collection = mbids.MbidCollection([recording, second_artist, first_artist])

    assert collection.mbids == [first_artist, second_artist, recording]
    assert collection.by_entity["artist"] == [first_artist.mbid, second_artist.mbid]
    assert collection.by_entity["recording"] == [recording.mbid]
    assert collection.by_entity["release"] == []
    assert collection.by_entity["release-group"] == []


def test_mbid_collection_hash_is_independent_of_input_order():
    artist = Mbid(mbid=uuid1(), entity="artist")
    recording = Mbid(mbid=uuid1(), entity="recording")

    first = mbids.MbidCollection([artist, recording])
    reordered = mbids.MbidCollection([recording, artist])
    different = mbids.MbidCollection([artist])

    assert first.collection_hash == reordered.collection_hash
    assert first.collection_hash != different.collection_hash
