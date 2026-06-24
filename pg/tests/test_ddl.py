import datetime
import re
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import Mapped, Session, mapped_column

from moomoo_pg import (
    TABLES,
    AnnotationQueueLog,
    BaseTable,
    ListenBrainzCollaborativeFilteringScore,
    ListenBrainzDataDump,
    ListenBrainzListen,
    LocalFileBirthTimestamp,
    LocalFileExcludeRegex,
    MusicBrainzDataDump,
    Playlist,
    PlaylistCollection,
    PlaylistTrack,
    execute_sql_fetchall,
    get_session,
)


class FakeTable(BaseTable):
    """Fake table for testing."""

    __tablename__ = "fake_table"
    a: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    b: Mapped[str] = mapped_column(nullable=False)


def test_tables_list():
    assert len(TABLES) > 5
    assert all(issubclass(table, BaseTable) for table in TABLES)


def test_create_drop_exists():
    """Make sure tables can be created, dropped, and checked for existence."""
    # silently do nothing if the table doesn't exist
    FakeTable.drop(if_exists=True)

    # should error since the table doesn't exist
    with pytest.raises(ProgrammingError):
        FakeTable.drop()

    # create the table
    FakeTable.create()
    assert FakeTable.exists()

    # silently do nothing if the table already exists
    FakeTable.create(if_not_exists=True)

    # drop the table
    FakeTable.drop()
    assert not FakeTable.exists()


def test_table_insert():
    """Make sure the insert method works as expected."""
    FakeTable.create()
    assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [{"count": 0}]

    FakeTable(a=1, b="a").insert()
    assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [{"count": 1}]

    # error if primary key is violated
    with pytest.raises(IntegrityError):
        FakeTable(a=1, b="b").insert()

    # error if not nullable is violated
    with pytest.raises(IntegrityError):
        FakeTable(a=2).insert()

    FakeTable(a=2, b="b").insert()
    assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [{"count": 2}]

    # does not commit if session is passed
    with get_session() as session:
        FakeTable(a=3, b="c").insert(session=session)
        assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [
            {"count": 2}
        ]


def test_table_bulk_insert():
    """Make sure the bulk_insert method works as expected."""
    FakeTable.create()
    assert len(FakeTable.select_star()) == 0

    FakeTable.bulk_insert([dict(a=1, b="a"), dict(a=2, b="b")])
    assert len(FakeTable.select_star()) == 2

    # error if primary key is violated
    with pytest.raises(IntegrityError):
        FakeTable.bulk_insert([dict(a=1, b="b")])

    # add two with the same key should raise an error, not insert either
    with pytest.raises(IntegrityError):
        FakeTable.bulk_insert([dict(a=8, b="b"), dict(a=8, b="c")])
    assert len(FakeTable.select_star()) == 2

    # error if not nullable is violated
    with pytest.raises(IntegrityError):
        FakeTable.bulk_insert([dict(a=4), dict(a=5)])

    assert len(FakeTable.select_star()) == 2

    # add some more
    FakeTable.bulk_insert([dict(a=6, b="d"), dict(a=7, b="e")])
    assert len(FakeTable.select_star()) == 4

    # does not commit if session is passed
    with get_session() as session:
        FakeTable.bulk_insert([dict(a=3, b="c")], session=session)
        assert len(FakeTable.select_star()) == 4


def test_table_upsert():
    """Make sure the upsert method works as expected."""
    FakeTable.create()
    sql = f"select b from {FakeTable.table_name()} where a = 1"
    assert execute_sql_fetchall(sql) == []

    FakeTable(a=1, b="a").insert()
    assert execute_sql_fetchall(sql) == [{"b": "a"}]

    with pytest.raises(IntegrityError):
        FakeTable(a=1, b="a").insert()

    # updates the row if primary key is violated
    FakeTable(a=1, b="b").upsert()
    assert execute_sql_fetchall(sql) == [{"b": "b"}]

    # adds a new row if primary key is not violated
    FakeTable(a=2, b="c").upsert(update_cols=["b"])
    assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [{"count": 2}]

    # does not commit if session is passed
    with get_session() as session:
        FakeTable(a=3, b="c").upsert(update_cols=["b"], session=session)
        assert execute_sql_fetchall(f"select count(1) from {FakeTable.table_name()}") == [
            {"count": 2}
        ]


def test_path_type():
    class PathTable(BaseTable):
        __tablename__ = "path_table"
        filepath: Mapped[Path] = mapped_column(primary_key=True, nullable=False)

    PathTable.create()
    PathTable(filepath=Path("a/b/c.mp3")).insert()
    with get_session() as session:
        row = session.query(PathTable).first()
        assert row.filepath == Path("a/b/c.mp3")


def test_ListenBrainzListen__last_listen_for_user():
    # none if no listens
    assert ListenBrainzListen.last_listen_for_user("a") is None

    # insert some listens
    tz = datetime.timezone.utc
    for year in 2021, 2022:
        listen = ListenBrainzListen(
            listen_md5=f"abc_{year}",
            username="a",
            json_data={"a": 1},
            listen_at_ts_utc=datetime.datetime(year, 1, 1, tzinfo=tz),
        )
        listen.insert()

    # correct last listen if listens
    assert ListenBrainzListen.last_listen_for_user("a") == datetime.datetime(2022, 1, 1, tzinfo=tz)


def test_LocalFileExcludeRegex__fetch_all_regex():
    # none if no regexes
    assert LocalFileExcludeRegex.fetch_all_regex() == []

    # insert some regexes
    for i in range(3):
        record = LocalFileExcludeRegex(
            pattern=f"^abc_{i}",
            note=f"note_{i}",
        )
        record.insert()

    # correct regexes if any
    assert LocalFileExcludeRegex.fetch_all_regex() == [
        re.compile("^abc_0"),
        re.compile("^abc_1"),
        re.compile("^abc_2"),
    ]


def test_ListenBrainzDataDump__replace_records(session: Session):
    dump = ListenBrainzDataDump(
        slug="test",
        ftp_path="test.tar.gz",
        ftp_modify_ts=datetime.datetime(2021, 1, 1),
        date=datetime.date(2021, 1, 1),
        start_timestamp=datetime.datetime(2021, 1, 1),
        end_timestamp=datetime.datetime(2021, 1, 2),
    )
    session.add(dump)
    session.commit()
    assert dump.records == []

    record = dict(user_id=1, artist_mbid=uuid4(), listen_count=1)
    dump.replace_records([record], session=session)
    session.commit()
    assert len(dump.records) == 1
    assert dump.records[0].user_id == 1

    dump.replace_records([], session=session)
    session.commit()
    assert len(dump.records) == 0


def test_ListenBrainzCollaborativeFilteringScore__reset_pk(session: Session):
    ListenBrainzCollaborativeFilteringScore.create(drop=True)
    # Insert a row to increment the PK
    ListenBrainzCollaborativeFilteringScore.bulk_insert(
        [
            dict(artist_mbid_a=uuid4(), artist_mbid_b=uuid4(), score_value=1.23),
            dict(artist_mbid_a=uuid4(), artist_mbid_b=uuid4(), score_value=1.23),
        ],
    )
    # assert that the PK is 1
    rows = ListenBrainzCollaborativeFilteringScore.select_star()
    assert [row["mbid_pair_id"] for row in rows] == [1, 2]

    # drop all rows in the table
    session.query(ListenBrainzCollaborativeFilteringScore).delete()
    session.commit()

    # add a row without specifying PK (should be 3)
    ListenBrainzCollaborativeFilteringScore.bulk_insert(
        [dict(artist_mbid_a=uuid4(), artist_mbid_b=uuid4(), score_value=2.34)]
    )
    rows = ListenBrainzCollaborativeFilteringScore.select_star()
    assert [row["mbid_pair_id"] for row in rows] == [3]

    # Reset the PK, Insert another row without specifying PK (should be 1 again)
    ListenBrainzCollaborativeFilteringScore.reset_pk()
    ListenBrainzCollaborativeFilteringScore.bulk_insert(
        [dict(artist_mbid_a=uuid4(), artist_mbid_b=uuid4(), score_value=2.34)]
    )
    rows = ListenBrainzCollaborativeFilteringScore.select_star()
    assert [row["mbid_pair_id"] for row in rows] == [3, 1]

    # does not commit if session is passed
    with get_session() as session:
        ListenBrainzCollaborativeFilteringScore.reset_pk(session=session)
        # uncommitted setval is visible within this same session
        tname = ListenBrainzCollaborativeFilteringScore.table_name()
        result = session.execute(
            text(f"SELECT nextval(pg_get_serial_sequence('{tname}', 'mbid_pair_id'))")
        )
        assert result.scalar() == 1

    # after rollback, the reset never took effect for other connections
    result = session.execute(
        text(f"SELECT nextval(pg_get_serial_sequence('{tname}', 'mbid_pair_id'))")
    )
    assert result.scalar() == 2


def test_MusicBrainzDataDump__replace_records(session: Session):
    dump = MusicBrainzDataDump(
        slug="test",
        packet_number=1,
        entity="artist",
        dump_timestamp=datetime.datetime(2021, 1, 1),
    )
    session.add(dump)
    session.commit()
    assert dump.records == []

    record = dict(mbid=str(uuid4()), json_data={})
    dump.replace_records([record], session=session)
    session.commit()
    assert len(dump.records) == 1
    assert dump.records[0].mbid == UUID(record["mbid"])

    dump.replace_records([], session=session)
    session.commit()
    assert len(dump.records) == 0


def test_AnnotationQueueLog__last_update_timestamp(session: Session):
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="fake") is None

    ts = datetime.datetime.now(datetime.timezone.utc)
    log = AnnotationQueueLog(source="fake", entity="artist", as_of_ts_utc=ts, queue_size=10)
    log.insert(session=session)
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="fake") == ts
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="other") is None


def test_LocalFileBirthTimestamp__bulk_upsert_on_conflict_do_nothing():
    LocalFileBirthTimestamp.create(drop=True)
    # Insert a row
    LocalFileBirthTimestamp.bulk_upsert_on_conflict_do_nothing(
        [
            dict(
                filepath="a/b/c.mp3",
                birth_at=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
            )
        ],
    )
    rows = LocalFileBirthTimestamp.select_star()
    assert len(rows) == 1
    assert rows[0]["filepath"] == "a/b/c.mp3"
    assert rows[0]["birth_at"] == datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)

    # Try to insert the same row again with a different birth_at (should do nothing)
    LocalFileBirthTimestamp.bulk_upsert_on_conflict_do_nothing(
        [
            dict(
                filepath="a/b/c.mp3",
                birth_at=datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
            )
        ],
    )
    rows = LocalFileBirthTimestamp.select_star()
    assert len(rows) == 1
    assert rows[0]["filepath"] == "a/b/c.mp3"
    assert rows[0]["birth_at"] == datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)

    # Insert a new row
    LocalFileBirthTimestamp.bulk_upsert_on_conflict_do_nothing(
        [
            dict(
                filepath="d/e/f.mp3",
                birth_at=datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
            )
        ]
    )
    rows = LocalFileBirthTimestamp.select_star()
    assert len(rows) == 2

    # does not commit if session is passed
    with get_session() as session:
        LocalFileBirthTimestamp.bulk_upsert_on_conflict_do_nothing(
            [
                dict(
                    filepath="g/h/i.mp3",
                    birth_at=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
                )
            ],
            session=session,
        )
    rows = LocalFileBirthTimestamp.select_star()
    assert len(rows) == 2


def test_PlaylistCollection__unique_constraint(session: Session):
    """Test the unique constraint on collection_name and username."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    # create a new collection with the same name
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    with pytest.raises(IntegrityError):
        session.commit()


def test_Playlist__unique_constraint(session: Session):
    """Test the unique constraint on collection_id and collection_order_index."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    item = Playlist(collection_id=collection.collection_id, collection_order_index=0, playlist=[])
    session.add(item)
    session.commit()

    # create a new item with the same collection_id and collection_order_index
    item = Playlist(collection_id=collection.collection_id, collection_order_index=0, playlist=[])
    session.add(item)
    with pytest.raises(IntegrityError):
        session.commit()


def test_PlaylistTrack__unique_constraint(session: Session):
    """Test the unique constraint on collection_id and collection_order_index."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    playlist = Playlist(collection_id=collection.collection_id, collection_order_index=0)
    session.add(playlist)
    session.commit()

    track = PlaylistTrack(track_order_index=0, playlist_id=playlist.playlist_id, filepath="a.mp3")
    session.add(track)
    session.commit()

    # add a new track with the same playlist_id and track_order_index
    track = PlaylistTrack(track_order_index=0, playlist_id=playlist.playlist_id, filepath="b.mp3")
    session.add(track)
    with pytest.raises(IntegrityError):
        session.commit()


def test_PlaylistCollection__get_collection_by_name(session: Session):
    # raised if collection does not exist
    with pytest.raises(ValueError):
        PlaylistCollection.get_collection_by_name(
            username="test", collection_name="test", session=session
        )

    # works if collection exists
    collection = PlaylistCollection(
        username="test", collection_name="test", refresh_at_hours_utc=[1, 2, 3]
    )
    session.add(collection)
    session.commit()

    result = PlaylistCollection.get_collection_by_name(
        username="test", collection_name="test", session=session
    )

    assert result.collection_id == collection.collection_id
    assert result.username == collection.username
    assert result.collection_name == collection.collection_name
    assert result.refresh_at_hours_utc == collection.refresh_at_hours_utc


def test_PlaylistCollection__last_refresh_target():
    """Test the last_refresh_target property."""
    # none if refresh_at_hours_utc is null
    collection = PlaylistCollection(username="test", collection_name="test")
    assert collection.last_refresh_target is None

    collection.refresh_at_hours_utc = [6, 18]

    # set now to 12:00, should return today's 6:00
    now = datetime.datetime(2024, 5, 18, 12, tzinfo=datetime.timezone.utc)
    with patch("moomoo_pg.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 6, tzinfo=datetime.timezone.utc
        )

    # set now to 00:00, should return yesterday's 18:00
    now = datetime.datetime(2024, 5, 18, 0, tzinfo=datetime.timezone.utc)
    with patch("moomoo_pg.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 17, 18, tzinfo=datetime.timezone.utc
        )
    # set now to 21:00, should return today's 18:00
    now = datetime.datetime(2024, 5, 18, 21, tzinfo=datetime.timezone.utc)
    with patch("moomoo_pg.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 18, tzinfo=datetime.timezone.utc
        )

    # exact match returns the same time
    now = datetime.datetime(2024, 5, 18, 6, tzinfo=datetime.timezone.utc)
    with patch("moomoo_pg.ddl.now_utc", return_value=now):
        assert collection.last_refresh_target == datetime.datetime(
            2024, 5, 18, 6, tzinfo=datetime.timezone.utc
        )


def test_PlaylistCollection__is_stale(session: Session):
    """Test the is_stale property."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()
    assert collection.is_stale

    # always stale if refresh_at_hours_utc is null
    collection.refreshed_at_utc = datetime.datetime.now(datetime.timezone.utc)
    assert collection.is_stale

    # set now to 12:00, refresh at 6:O
    collection.refresh_at_hours_utc = [6]
    now = datetime.datetime(2024, 5, 18, 12, tzinfo=datetime.timezone.utc)
    with patch("moomoo_pg.ddl.now_utc", return_value=now):
        # stale if target at 6:00, refreshed at 5:00
        collection.refreshed_at_utc = now.replace(hour=5)
        assert collection.is_stale

        # not stale if target at 6:00, refreshed at 6:00
        collection.refreshed_at_utc = now.replace(hour=6)
        assert not collection.is_stale

        # not stale if target at 6:00, refreshed at 7:00
        collection.refreshed_at_utc = now.replace(hour=7)
        assert not collection.is_stale


def test_PlaylistCollection__ordered_items(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    # empty if no items
    assert collection.ordered_items == []

    p1 = Playlist(collection_id=collection.collection_id, collection_order_index=2)
    p2 = Playlist(collection_id=collection.collection_id, collection_order_index=0)
    p3 = Playlist(collection_id=collection.collection_id, collection_order_index=1)
    session.add_all([p1, p2, p3])
    session.commit()

    assert [p.collection_order_index for p in collection.ordered_items] == [0, 1, 2]


def test_PlaylistCollection__make_playlist(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    playlist = collection.make_playlist(tracks=[])
    assert playlist.collection_id == collection.collection_id
    assert playlist.collection_order_index == 0
    session.add(playlist)
    session.commit()

    playlist2 = collection.make_playlist(tracks=[])
    assert playlist2.collection_id == collection.collection_id
    assert playlist2.collection_order_index == 1


def test_PlaylistCollection__replace_playlists(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    playlist = collection.make_playlist(tracks=[], title="playlist1").data
    collection.replace_playlists([playlist], session=session)
    session.commit()
    assert len(collection.items) == 1
    assert collection.items[0].title == "playlist1"

    # replace with a new playlist
    playlist = collection.make_playlist(tracks=[], title="playlist2").data
    collection.replace_playlists([playlist], session=session)
    session.commit()
    assert len(collection.items) == 1
    assert collection.items[0].title == "playlist2"

    # test stale handler
    collection = PlaylistCollection(
        username="test2", collection_name="test2", refresh_at_hours_utc=[12]
    )
    session.add(collection)
    session.commit()

    # no updates yet
    assert collection.is_stale
    playlist = collection.make_playlist(tracks=[], title="playlist1").data
    collection.replace_playlists([playlist], session=session)
    session.commit()
    assert collection.is_fresh

    # test force handler
    assert collection.replace_playlists([playlist], session=session) is False
    assert collection.replace_playlists([playlist], session=session, force=True) is True


def test_Playlist__Data_roundtrip():
    playlist = Playlist(
        collection_id=uuid4(),
        collection_order_index=0,
        tracks=[],
        title="playlist1",
        description="description1",
    )

    roundtrip = Playlist.from_data(
        playlist.data,
        collection_id=playlist.collection_id,
        collection_order_index=playlist.collection_order_index,
    )
    assert roundtrip.collection_id == playlist.collection_id
    assert roundtrip.collection_order_index == playlist.collection_order_index
    assert roundtrip.title == playlist.title
    assert roundtrip.description == playlist.description


def test_Playlist__ordered_tracks():
    playlist = Playlist(
        collection_id=uuid4(),
        collection_order_index=0,
        tracks=[],
        title="playlist1",
        description="description1",
    )
    assert playlist.ordered_tracks == []

    # add some tracks
    playlist.tracks = [
        PlaylistTrack(track_order_index=2, filepath=Path("c.mp3")),
        PlaylistTrack(track_order_index=0, filepath=Path("a.mp3")),
        PlaylistTrack(track_order_index=1, filepath=Path("b.mp3")),
    ]
    assert [t.filepath.name for t in playlist.ordered_tracks] == ["a.mp3", "b.mp3", "c.mp3"]


def test_Playlist__seeds():
    playlist = Playlist(
        collection_id=uuid4(),
        collection_order_index=0,
        tracks=[],
        title="playlist1",
        description="description1",
    )
    assert playlist.seeds == []

    # add some tracks
    playlist.tracks = [
        PlaylistTrack(track_order_index=0, filepath=Path("a.mp3"), is_seed=True),
        PlaylistTrack(track_order_index=1, filepath=Path("b.mp3"), is_seed=False),
        PlaylistTrack(track_order_index=2, filepath=Path("c.mp3"), is_seed=True),
    ]
    assert [t.filepath for t in playlist.seeds] == [Path("a.mp3"), Path("c.mp3")]


def test_Playlist__from_tracks():
    tracks = [PlaylistTrack.Data(filepath="a.mp3"), PlaylistTrack.Data(filepath="b.mp3")]
    collection_id = uuid4()
    playlist = Playlist.from_tracks(
        tracks=tracks,
        collection_id=collection_id,
        collection_order_index=0,
        title="playlist1",
        description="description1",
    )
    assert [i.track_order_index for i in playlist.tracks] == [0, 1]
    assert [i.filepath.name for i in playlist.tracks] == ["a.mp3", "b.mp3"]
    assert playlist.collection_id == collection_id
    assert playlist.collection_order_index == 0
    assert playlist.title == "playlist1"
    assert playlist.description == "description1"

    # check that we have equivalence in from_data
    assert (
        playlist.data
        == Playlist.from_data(
            playlist.data,
            collection_id=playlist.collection_id,
            collection_order_index=playlist.collection_order_index,
        ).data
    )

    # sneaky test that the inputs to from_data can also be Playlist, not just playlist.data, due to
    # the model dump override
    assert (
        playlist.data
        == Playlist.from_data(
            playlist,  # this is a playlist object, not a Playlist.Data object
            collection_id=playlist.collection_id,
            collection_order_index=playlist.collection_order_index,
        ).data
    )


def test_Playlist__append_tracks(session: Session):
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    playlist = Playlist(
        collection_id=collection.collection_id,
        collection_order_index=0,
        tracks=[],
        title="playlist1",
        description="description1",
    )
    session.add(playlist)
    session.commit()

    tracks = [PlaylistTrack.Data(filepath="a.mp3"), PlaylistTrack.Data(filepath="b.mp3")]
    playlist.append_tracks(tracks=tracks, session=session)
    session.commit()
    assert [i.track_order_index for i in playlist.tracks] == [0, 1]
    assert [i.filepath.name for i in playlist.tracks] == ["a.mp3", "b.mp3"]

    # add more
    tracks = [PlaylistTrack.Data(filepath="c.mp3"), PlaylistTrack.Data(filepath="d.mp3")]
    playlist.append_tracks(tracks=tracks, session=session)
    session.commit()
    assert [i.track_order_index for i in playlist.tracks] == [0, 1, 2, 3]
    assert [i.filepath.name for i in playlist.tracks] == ["a.mp3", "b.mp3", "c.mp3", "d.mp3"]


def test_PlaylistTrack__Data_roundtrip():
    track = PlaylistTrack(track_order_index=0, filepath=Path("a.mp3"), is_seed=True)

    roundtrip = PlaylistTrack.from_data(track.data, track_order_index=track.track_order_index)
    assert roundtrip.track_order_index == track.track_order_index
    assert roundtrip.filepath == track.filepath
    assert roundtrip.is_seed == track.is_seed

    # sneaky test that the inputs can also be PlaylistTrack, not just .data, due to the model dump
    assert (
        track.data == PlaylistTrack.from_data(track, track_order_index=track.track_order_index).data
    )


def test_PlaylistTrack__to_dict():
    track = PlaylistTrack(
        track_order_index=0, filepath=Path("a.mp3"), is_seed=True, match_distance=0.5
    )
    assert track.to_dict() == {
        "filepath": "a.mp3",
        "is_seed": True,
        "match_distance": 0.5,
    }
