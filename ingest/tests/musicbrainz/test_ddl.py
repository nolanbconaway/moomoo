from uuid import uuid1

from moomoo_pg import get_session
from sqlalchemy import text

from moomoo_ingest.annotate_mbids import Mbid
from moomoo_ingest.musicbrainz.ddl import (
    Artist,
    ArtistCredit,
    ArtistCreditName,
    ArtistTag,
    Medium,
    Recording,
    RecordingTag,
    Release,
    ReleaseFirstReleaseDate,
    ReleaseGroup,
    ReleaseGroupMeta,
    ReleaseGroupSecondaryTypeJoin,
    ReleaseGroupTag,
    ReleaseStatus,
    ReleaseTag,
    Tag,
    Track,
    copy_rows,
)
from moomoo_ingest.musicbrainz.mbids import MbidCollection


def create_destination_table(session, schema: str, table: str):
    session.execute(text(f"CREATE SCHEMA {schema}"))
    session.execute(text(f"CREATE TABLE {schema}.{table} (LIKE musicbrainz.{table} INCLUDING ALL)"))


def test_copy_rows():
    suffix = uuid1().hex
    src_table = f"copy_rows_source_{suffix}"
    dst_table = f"copy_rows_target_{suffix}"

    with get_session() as mbz_session, get_session() as app_session:
        app_session.execute(text(f"CREATE TABLE musicbrainz.{src_table} (id integer)"))
        app_session.execute(text(f"CREATE TABLE musicbrainz.{dst_table} (id integer)"))
        app_session.execute(
            text(f"INSERT INTO musicbrainz.{src_table} VALUES (:id)"),
            [{"id": 1}, {"id": 2}],
        )
        app_session.commit()

        copy_rows(
            mbz_session=mbz_session,
            app_session=app_session,
            select_sql=f"SELECT id FROM musicbrainz.{src_table} WHERE id >= %(min_id)s",
            params={"min_id": 2},
            target_table=dst_table,
        )
        app_session.commit()

        result = app_session.execute(text(f"SELECT id FROM musicbrainz.{dst_table}")).all()
        assert result == [(2,)]


def test_MusicBrainzBase__copy__full_table():
    dest_schema = f"copy_full_{uuid1().hex}"
    gid = uuid1()

    with get_session() as mbz_session, get_session() as app_session:
        create_destination_table(app_session, dest_schema, "release_status")
        mbz_session.execute(
            text("""
                INSERT INTO musicbrainz.release_status (id, gid, name, child_order)
                VALUES (:id, :gid, :name, :child_order)
            """),
            {"id": 1, "gid": gid, "name": "Official", "child_order": 1},
        )

        ReleaseStatus.copy(
            mbz_session=mbz_session,
            app_session=app_session,
            mbids=MbidCollection([]),
            dest_schema=dest_schema,
        )
        app_session.commit()

        result = app_session.execute(
            text(f"SELECT id, gid, name, child_order FROM {dest_schema}.release_status")
        ).all()
        assert result == [(1, gid, "Official", 1)]


def test_MusicBrainzBase__copy__mbid_scoped_table():
    dest_schema = f"copy_scoped_{uuid1().hex}"
    gid = uuid1()

    with get_session() as mbz_session, get_session() as app_session:
        create_destination_table(app_session, dest_schema, "artist")
        mbz_session.execute(
            text("""
                INSERT INTO musicbrainz.artist (id, gid, name, sort_name, comment)
                VALUES (:id, :gid, :name, :sort_name, :comment)
            """),
            {"id": 1, "gid": gid, "name": "Artist", "sort_name": "Artist", "comment": ""},
        )

        Artist.copy(
            mbz_session=mbz_session,
            app_session=app_session,
            mbids=MbidCollection([Mbid(mbid=gid, entity="artist")]),
            dest_schema=dest_schema,
        )
        app_session.commit()

        result = app_session.execute(text(f"SELECT id, gid, name FROM {dest_schema}.artist")).all()
        assert result == [(1, gid, "Artist")]


def test_ArtistCredit__ids_to_copy(monkeypatch):
    release_group_id = 10  # Returned by the mocked release-group selector.
    release_group_artist_credit_id = 100  # Credit read from the selected release group.
    release_id = 11  # Returned by the mocked release selector.
    release_artist_credit_id = 101  # Credit read from the selected release.
    recording_id = 22  # Returned by the mocked recording selector.
    recording_artist_credit_id = 202  # Credit read from the selected recording.
    track_id = 33  # Track associated with the selected recording.
    track_artist_credit_id = 303  # Credit read from that track.
    monkeypatch.setattr(
        ReleaseGroup, "ids_to_copy", classmethod(lambda cls, *_, **__: {release_group_id})
    )
    monkeypatch.setattr(Release, "ids_to_copy", classmethod(lambda cls, *_, **__: {release_id}))
    monkeypatch.setattr(Recording, "ids_to_copy", classmethod(lambda cls, *_, **__: {recording_id}))

    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.release_group
                    (id, gid, name, artist_credit, comment)
                VALUES (:id, :gid, 'Group', :artist_credit_id, '')
            """),
            {
                "id": release_group_id,
                "gid": uuid1(),
                "artist_credit_id": release_group_artist_credit_id,
            },
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.release
                    (id, gid, name, artist_credit, release_group, comment, quality)
                VALUES (:id, :gid, 'Release', :artist_credit_id, 1, '', 1)
            """),
            {"id": release_id, "gid": uuid1(), "artist_credit_id": release_artist_credit_id},
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.recording
                    (id, gid, name, artist_credit, comment, video)
                VALUES (:id, :gid, 'Recording', :artist_credit_id, '', false)
            """),
            {"id": recording_id, "gid": uuid1(), "artist_credit_id": recording_artist_credit_id},
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.track
                    (id
                    , gid
                    , recording
                    , medium
                    , position
                    , number
                    , name
                    , artist_credit
                    , is_data_track
                    )
                VALUES (:id, :gid, :recording_id, 1, 1, '1', 'Track', :artist_credit_id, false)
            """),
            {
                "id": track_id,
                "gid": uuid1(),
                "recording_id": recording_id,
                "artist_credit_id": track_artist_credit_id,
            },
        )

        result = ArtistCredit.ids_to_copy(session, MbidCollection([]))

    assert result == {
        release_group_artist_credit_id,
        release_artist_credit_id,
        recording_artist_credit_id,
        track_artist_credit_id,
    }


def test_ArtistCreditName__ids_to_copy(monkeypatch):
    # artist credit name just passes to artist credit, so mock it and assert we get the same result.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        ArtistCredit,
        "ids_to_copy",
        classmethod(lambda cls, *_, **__: expected_ids),
    )

    assert ArtistCreditName.ids_to_copy(None, collection) == expected_ids


def test_Release__ids_to_copy(monkeypatch):
    artist_id = 42  # Returned by the stubbed artist lookup.
    direct_release_id = 11  # Selected by the release MBID.
    artist_release_id = 22  # Selected through the artist credit.
    group_release_id = 33  # Selected through the release-group MBID.
    group_sibling_release_id = 34  # Pulled in as a sibling release.
    target_release_group_id = 503  # Shared release group for both releases above.
    direct_release_mbid = uuid1()
    target_release_group_mbid = uuid1()

    release_rows = [
        (direct_release_id, direct_release_mbid, 101, 501),
        (artist_release_id, uuid1(), 102, 502),
        (group_release_id, uuid1(), 103, target_release_group_id),
        (group_sibling_release_id, uuid1(), 104, target_release_group_id),
    ]

    monkeypatch.setattr(Artist, "ids_to_copy", classmethod(lambda cls, *_, **__: {artist_id}))

    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.release
                    (id, gid, name, artist_credit, release_group, comment, quality)
                VALUES (:id, :gid, 'Release', :artist_credit, :release_group, '', 1)
            """),
            [
                dict(id=id_, gid=gid, artist_credit=credit_id, release_group=group_id)
                for id_, gid, credit_id, group_id in release_rows
            ],
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.artist_credit_name
                    (artist_credit, position, artist, name, join_phrase)
                VALUES (102, 0, :artist_id, 'Artist', '')
            """),
            {"artist_id": artist_id},
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.release_group
                    (id, gid, name, artist_credit, comment)
                VALUES (:id, :gid, 'Group', 103, '')
            """),
            {"id": target_release_group_id, "gid": target_release_group_mbid},
        )

        result = Release.ids_to_copy(
            session,
            MbidCollection(
                [
                    Mbid(mbid=direct_release_mbid, entity="release"),
                    Mbid(mbid=target_release_group_mbid, entity="release-group"),
                ]
            ),
        )

    assert result == {
        direct_release_id,
        artist_release_id,
        group_release_id,
        group_sibling_release_id,
    }


def test_ReleaseGroup__ids_to_copy(monkeypatch):
    direct_group_id = 31  # Matched directly from the release-group MBID.
    release_group_id = 32  # Found through the selected release.
    release_id = 44  # Returned by the mocked release selector.
    direct_group_mbid = uuid1()
    monkeypatch.setattr(Release, "ids_to_copy", classmethod(lambda cls, *_, **__: {release_id}))

    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.release_group (id, gid, name, artist_credit, comment)
                VALUES (:id, :gid, 'Group', 1, '')
            """),
            [
                {"id": direct_group_id, "gid": direct_group_mbid},
                {"id": release_group_id, "gid": uuid1()},
            ],
        )
        session.execute(
            text("""
                INSERT INTO musicbrainz.release
                    (id, gid, name, artist_credit, release_group, comment, quality)
                VALUES (:id, :gid, 'Release', 1, :release_group_id, '', 1)
            """),
            {"id": release_id, "gid": uuid1(), "release_group_id": release_group_id},
        )

        result = ReleaseGroup.ids_to_copy(
            session,
            MbidCollection([Mbid(mbid=direct_group_mbid, entity="release-group")]),
        )

    assert result == {direct_group_id, release_group_id}


def test_ReleaseFirstReleaseDate__ids_to_copy(monkeypatch):
    # first release date just passes to Release.ids_to_copy, so check that we get the same result.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        Release,
        "ids_to_copy",
        classmethod(lambda cls, *_, **__: expected_ids),
    )

    assert ReleaseFirstReleaseDate.ids_to_copy(None, collection) == expected_ids


def test_Artist__ids_to_copy():
    artist_id = 12
    artist_mbid = uuid1()
    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.artist (id, gid, name, sort_name, comment)
                VALUES
                    (:artist_id, :artist_mbid, 'Artist', 'Artist', ''),
                    (13, :other_mbid, 'Other', 'Other', '')
            """),
            {"artist_id": artist_id, "artist_mbid": artist_mbid, "other_mbid": uuid1()},
        )

        result = Artist.ids_to_copy(
            session,
            MbidCollection([Mbid(mbid=artist_mbid, entity="artist")]),
        )

    assert result == {artist_id}


def test_Recording__ids_to_copy():
    recording_id = 24
    recording_mbid = uuid1()
    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.recording
                    (id, gid, name, artist_credit, comment, video)
                VALUES
                    (:recording_id, :recording_mbid, 'Recording', 1, '', false),
                    (25, :other_mbid, 'Other', 2, '', false)
            """),
            {"recording_id": recording_id, "recording_mbid": recording_mbid, "other_mbid": uuid1()},
        )

        result = Recording.ids_to_copy(
            session,
            MbidCollection([Mbid(mbid=recording_mbid, entity="recording")]),
        )

    assert result == {recording_id}


def test_ArtistTag__ids_to_copy(monkeypatch):
    # ArtistTag delegates to Artist.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(Artist, "ids_to_copy", classmethod(lambda cls, *_, **__: expected_ids))

    assert ArtistTag.ids_to_copy(None, collection) == expected_ids


def test_ReleaseTag__ids_to_copy(monkeypatch):
    # ReleaseTag delegates to Release.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(Release, "ids_to_copy", classmethod(lambda cls, *_, **__: expected_ids))

    assert ReleaseTag.ids_to_copy(None, collection) == expected_ids


def test_RecordingTag__ids_to_copy(monkeypatch):
    # RecordingTag delegates to Recording.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(Recording, "ids_to_copy", classmethod(lambda cls, *_, **__: expected_ids))

    assert RecordingTag.ids_to_copy(None, collection) == expected_ids


def test_ReleaseGroupTag__ids_to_copy(monkeypatch):
    # ReleaseGroupTag delegates to ReleaseGroup.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        ReleaseGroup, "ids_to_copy", classmethod(lambda cls, *_, **__: expected_ids)
    )

    assert ReleaseGroupTag.ids_to_copy(None, collection) == expected_ids


def test_ReleaseGroupMeta__ids_to_copy(monkeypatch):
    # ReleaseGroupMeta delegates to ReleaseGroup.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        ReleaseGroup,
        "ids_to_copy",
        classmethod(lambda cls, *_, **__: expected_ids),
    )

    assert ReleaseGroupMeta.ids_to_copy(None, collection) == expected_ids


def test_Medium__ids_to_copy(monkeypatch):
    recording_id = 17
    medium_ids = {31, 32}
    monkeypatch.setattr(Recording, "ids_to_copy", classmethod(lambda cls, *_, **__: {recording_id}))
    tracks = [
        (101, recording_id, 31),
        (102, recording_id, 32),
        (103, 99, 33),
    ]

    with get_session() as session:
        session.execute(
            text("""
                INSERT INTO musicbrainz.track
                    (id
                    , gid
                    , recording
                    , medium
                    , position
                    , number
                    , name
                    , artist_credit
                    , is_data_track
                    )
                VALUES (:id, :gid, :recording_id, :medium_id, 1, '1', 'Track', 1, false)
            """),
            [
                {"id": id_, "gid": uuid1(), "recording_id": record_id, "medium_id": medium_id}
                for id_, record_id, medium_id in tracks
            ],
        )

        result = Medium.ids_to_copy(session, MbidCollection([]))

    assert result == medium_ids


def test_Track__ids_to_copy(monkeypatch):
    # Track delegates to Recording.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        Recording,
        "ids_to_copy",
        classmethod(lambda cls, *_, **__: expected_ids),
    )

    assert Track.ids_to_copy(None, collection) == expected_ids


def test_ReleaseGroupSecondaryTypeJoin__ids_to_copy(monkeypatch):
    # ReleaseGroupSecondaryTypeJoin delegates to ReleaseGroup.ids_to_copy.
    collection = MbidCollection([])
    expected_ids = {-13, 101, 867_5309}
    monkeypatch.setattr(
        ReleaseGroup,
        "ids_to_copy",
        classmethod(lambda cls, *_, **__: expected_ids),
    )

    assert ReleaseGroupSecondaryTypeJoin.ids_to_copy(None, collection) == expected_ids


def test_Tag__ids_to_copy(monkeypatch):
    source_ids = {
        Artist: 101,
        Release: 102,
        Recording: 103,
        ReleaseGroup: 104,
    }
    tag_rows = (
        ("artist_tag", "artist", 101, 901),
        ("release_tag", "release", 102, 902),
        ("recording_tag", "recording", 103, 903),
        ("release_group_tag", "release_group", 104, 904),
    )
    for model, entity_id in source_ids.items():
        monkeypatch.setattr(
            model,
            "ids_to_copy",
            classmethod(lambda cls, *_, result=entity_id, **__: {result}),
        )

    with get_session() as session:
        for table, column, entity_id, tag_id in tag_rows:
            session.execute(
                text(f"""
                    INSERT INTO musicbrainz.{table} ({column}, tag, count)
                    VALUES (:entity_id, :tag_id, 1)
                    , (999, 9999, 1)
                """),
                {"entity_id": entity_id, "tag_id": tag_id},
            )

        result = Tag.ids_to_copy(session, MbidCollection([]))

    assert result == {901, 902, 903, 904}
