import psycopg
import pytest
from moomoo_http.app import create_app
from moomoo_http.db import Base, db, execute_sql_fetchall
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Mapped, mapped_column


class FakeTable(Base):
    """Fake table for testing."""

    __tablename__ = "fake_table"
    a: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    b: Mapped[str] = mapped_column(nullable=False)


@pytest.fixture(autouse=True)
def app_context():
    """Make sure the app context is created for each test."""
    with create_app().app_context():
        yield


def test_pg_connect_mocked(postgresql: psycopg.Connection):
    """Make sure the pg_connect function is mocked as expected.

    The postgresql fixture is provided by the pytest-postgresql plugin, and
    points to a fresh, temporary database.
    """
    assert db.engine.url.username == postgresql.info.user
    assert db.engine.url.host == postgresql.info.host
    assert db.engine.url.port == postgresql.info.port
    assert db.engine.url.database == postgresql.info.dbname


def test_execute_sql_fetchall():
    """Make sure the execute_sql_fetchall function works as expected."""
    res = execute_sql_fetchall(
        sql="select 1 as a union select 2 as a", session=db.session
    )
    assert res == [{"a": 1}, {"a": 2}]
    assert isinstance(res, list)
    assert isinstance(res[0], dict)
    assert isinstance(res[0]["a"], int)
    assert isinstance(next(iter(res[0].keys())), str)

    # params
    res = execute_sql_fetchall(
        sql="select :a as a", params=dict(a=1), session=db.session
    )
    assert res == [{"a": 1}]


def test_can_create_tables():
    """Make sure we can create tables."""
    # does not exist
    with pytest.raises(ProgrammingError):
        execute_sql_fetchall(sql="select * from fake_table", session=db.session)

    # create
    Base.metadata.create_all(db.engine)
    db.session.commit()

    # exists
    assert (
        execute_sql_fetchall(sql="select * from fake_table", session=db.session) == []
    )
