import datetime
import re
from unittest.mock import patch
from uuid import UUID, uuid1

import pytest
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
    PlaylistCollection,
    PlaylistCollectionItem,
    execute_sql_fetchall,
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

    record = dict(user_id=1, artist_mbid=uuid1(), listen_count=1)
    dump.replace_records([record], session=session)
    assert len(dump.records) == 1
    assert dump.records[0].user_id == 1

    dump.replace_records([], session=session)
    assert len(dump.records) == 0


def test_ListenBrainzCollaborativeFilteringScore__reset_pk(session: Session):
    ListenBrainzCollaborativeFilteringScore.create(drop=True)
    # Insert a row to increment the PK
    ListenBrainzCollaborativeFilteringScore.bulk_insert(
        [
            dict(artist_mbid_a=uuid1(), artist_mbid_b=uuid1(), score_value=1.23),
            dict(artist_mbid_a=uuid1(), artist_mbid_b=uuid1(), score_value=1.23),
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
        [dict(artist_mbid_a=uuid1(), artist_mbid_b=uuid1(), score_value=2.34)]
    )
    rows = ListenBrainzCollaborativeFilteringScore.select_star()
    assert [row["mbid_pair_id"] for row in rows] == [3]

    # Reset the PK, Insert another row without specifying PK (should be 1 again)
    ListenBrainzCollaborativeFilteringScore.reset_pk()
    ListenBrainzCollaborativeFilteringScore.bulk_insert(
        [dict(artist_mbid_a=uuid1(), artist_mbid_b=uuid1(), score_value=2.34)]
    )
    rows = ListenBrainzCollaborativeFilteringScore.select_star()
    assert [row["mbid_pair_id"] for row in rows] == [3, 1]


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

    record = dict(mbid=str(uuid1()), json_data={})
    dump.replace_records([record], session=session)
    assert len(dump.records) == 1
    assert dump.records[0].mbid == UUID(record["mbid"])

    dump.replace_records([], session=session)
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


def test_PlaylistCollectionItem__unique_constraint(session: Session):
    """Test the unique constraint on collection_id and collection_order_index."""
    collection = PlaylistCollection(username="test", collection_name="test")
    session.add(collection)
    session.commit()

    item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=[],
    )
    session.add(item)
    session.commit()

    # create a new item with the same collection_id and collection_order_index
    item = PlaylistCollectionItem(
        collection_id=collection.collection_id,
        collection_order_index=0,
        playlist=[],
    )
    session.add(item)
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
