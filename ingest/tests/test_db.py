import datetime
import re
from pathlib import Path
from uuid import UUID, uuid1

import psycopg
import pytest
from click.testing import CliRunner
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import Mapped, mapped_column

from moomoo_ingest.db.cli import cli as db_cli
from moomoo_ingest.db.connection import execute_sql_fetchall, get_engine, get_session
from moomoo_ingest.db.ddl import (
    TABLES,
    AnnotationQueueLog,
    BaseTable,
    ListenBrainzCollaborativeFilteringScore,
    ListenBrainzDataDump,
    ListenBrainzDataDumpRecord,
    ListenBrainzListen,
    LocalFileExcludeRegex,
    MusicBrainzDataDump,
    MusicBrainzDataDumpRecord,
)


class FakeTable(BaseTable):
    """Fake table for testing."""

    __tablename__ = "fake_table"
    a: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    b: Mapped[str] = mapped_column(nullable=False)


def test_pg_connect_mocked(postgresql: psycopg.Connection):
    """Make sure the pg_connect function is mocked as expected.

    The postgresql fixture is provided by the pytest-postgresql plugin, and
    points to a fresh, temporary database.
    """
    engine = get_engine()
    assert engine.url.username == postgresql.info.user
    assert engine.url.host == postgresql.info.host
    assert engine.url.port == postgresql.info.port
    assert engine.url.database == postgresql.info.dbname


def test_execute_sql_fetchall():
    """Make sure the execute_sql_fetchall function works as expected."""
    res = execute_sql_fetchall("select 1 as a union select 2 as a")
    assert res == [{"a": 1}, {"a": 2}]
    assert isinstance(res, list)
    assert isinstance(res[0], dict)
    assert isinstance(res[0]["a"], int)
    assert isinstance(next(iter(res[0].keys())), str)

    # params
    res = execute_sql_fetchall("select :a as a", params=dict(a=1))
    assert res == [{"a": 1}]

    # conn
    with get_session() as session:
        execute_sql_fetchall("create temp table t (a int)", session=session)
        execute_sql_fetchall("insert into t values (1), (2)", session=session)
        res = execute_sql_fetchall("select * from t order by a", session=session)
        assert res == [{"a": 1}, {"a": 2}]


def test_create_drop_exists():
    """Make sure tables can be created, dropped, and checked for existence."""
    # silently do nothing if the table doesn't exist
    assert not ListenBrainzListen.exists()
    ListenBrainzListen.drop(if_exists=True)

    # should error since the table doesn't exist
    with pytest.raises(ProgrammingError):
        ListenBrainzListen.drop()

    # create the table
    ListenBrainzListen.create()
    assert ListenBrainzListen.exists()

    # silently do nothing if the table already exists
    ListenBrainzListen.create(if_not_exists=True)

    # drop the table
    ListenBrainzListen.drop()
    assert not ListenBrainzListen.exists()


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
    ListenBrainzListen.create()

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
    LocalFileExcludeRegex.create()

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


def test_ListenBrainzDataDump__replace_records():
    session = get_session()
    ListenBrainzDataDump.create()
    ListenBrainzDataDumpRecord.create()

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


def test_cli__ddl():
    runner = CliRunner()
    valid_table = TABLES[0].__tablename__
    invalid_table = "nonexistent_table"

    # error if all and table_name are both specified
    res = runner.invoke(db_cli, ["ddl", "--all", valid_table])
    assert res.exit_code != 0
    assert "Must specify either --all or a table name, not both." in res.stdout

    # error if neither all nor table_name are specified
    res = runner.invoke(db_cli, ["ddl"])
    assert res.exit_code != 0
    assert "Must specify either --all or a table name." in res.stdout

    # error if table_name is not a valid table name
    res = runner.invoke(db_cli, ["ddl", invalid_table])
    assert res.exit_code != 0
    assert "Error: Invalid value for " in res.stdout
    assert "'nonexistent_table' is not one of" in res.stdout

    # print ddl for all tables
    res = runner.invoke(db_cli, ["ddl", "--all"])
    assert res.exit_code == 0
    assert res.stdout.count("CREATE TABLE") == len(TABLES)

    # print ddl for one table
    res = runner.invoke(db_cli, ["ddl", valid_table])
    assert res.exit_code == 0
    assert res.stdout.count("CREATE TABLE") == 1


def test_cli__create():
    runner = CliRunner()
    table = TABLES[0]
    table_name = table.__tablename__
    invalid_table_name = "nonexistent_table"

    # error if table exists and if_not_exists/drop are not specified
    table.create()
    assert table.exists()

    res = runner.invoke(db_cli, ["create", table_name])
    assert res.exit_code != 0
    assert isinstance(res.exception, ProgrammingError)
    assert "already exists" in str(res.exception)

    # silently do nothing if table exists and if_not_exists is specified
    assert table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--if-not-exists"])
    assert res.exit_code == 0

    # drop the table
    assert table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--drop"])
    assert res.exit_code == 0

    # silently do nothing if table does not exist and drop is specified
    table.drop()
    assert not table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--drop"])
    assert res.exit_code == 0

    # error if invalid table name is specified
    res = runner.invoke(db_cli, ["create", invalid_table_name])
    assert res.exit_code != 0
    assert "Error: Invalid value for " in res.stdout
    assert "'nonexistent_table' is not one of" in res.stdout


def test_cli__add_exclude_path(tmp_path: Path):
    LocalFileExcludeRegex.create()
    runner = CliRunner()

    target = tmp_path / "target"
    target.mkdir()

    # error if no library is specified
    res = runner.invoke(db_cli, ["add-exclude-path", str(target)])
    assert res.exit_code != 0
    assert "Error: Missing option '--library'" in res.stdout

    # error if library does not exist
    res = runner.invoke(db_cli, ["add-exclude-path", str(target), "--library", "nope"])
    assert res.exit_code != 0
    assert "Error: Invalid value for '--library': Directory 'nope' does not exist." in res.stdout

    # error if path does not exist
    res = runner.invoke(db_cli, ["add-exclude-path", "nope", "--library", str(tmp_path)])
    assert res.exit_code != 0
    assert "Error: Invalid value for 'PATH': Path 'nope' does not exist." in res.stdout

    # error if path is the library
    res = runner.invoke(db_cli, ["add-exclude-path", str(tmp_path), "--library", str(tmp_path)])
    assert res.exit_code != 0
    assert "Cannot exclude the media library path." in res.stdout

    # add a path
    res = runner.invoke(db_cli, ["add-exclude-path", str(target), "--library", str(tmp_path)])
    assert res.exit_code == 0
    assert LocalFileExcludeRegex.fetch_all_regex() == [re.compile("^target")]

    # add one with special characters
    target = tmp_path / "target (1)"
    target.mkdir()
    res = runner.invoke(db_cli, ["add-exclude-path", str(target), "--library", str(tmp_path)])
    assert res.exit_code == 0
    assert LocalFileExcludeRegex.fetch_all_regex() == [
        re.compile("^target"),
        re.compile("^target\\ \\(1\\)"),
    ]


def test_ListenBrainzCollaborativeFilteringScore__reset_pk():
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
    with get_session() as session:
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


def test_MusicBrainzDataDump__replace_records():
    session = get_session()
    MusicBrainzDataDump.create()
    MusicBrainzDataDumpRecord.create()

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


def test_AnnotationQueueLog__last_update_timestamp():
    AnnotationQueueLog.create()
    session = get_session()
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="fake") is None

    ts = datetime.datetime.now(datetime.timezone.utc)
    log = AnnotationQueueLog(source="fake", entity="artist", as_of_ts_utc=ts, queue_size=10)
    log.insert(session=session)
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="fake") == ts
    assert AnnotationQueueLog.last_update_timestamp(session=session, source="other") is None
