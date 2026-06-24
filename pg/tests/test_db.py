from unittest.mock import patch

import pytest
from psycopg.errors import UndefinedTable
from sqlalchemy.exc import ProgrammingError, StatementError
from sqlalchemy.orm import Session

from moomoo_pg import db_retry, execute_sql_fetchall, get_session, make_temp_table


def test_execute_sql_fetchall():
    """Make sure the execute_sql_fetchall function works as expected."""
    res = execute_sql_fetchall("select 1 as a union select 2 as a")
    assert res == [{"a": 1}, {"a": 2}]

    # params
    res = execute_sql_fetchall("select :a as a", params=dict(a=1))
    assert res == [{"a": 1}]

    # conn
    with get_session() as session:
        execute_sql_fetchall("create temp table t (a int)", session=session)
        execute_sql_fetchall("insert into t values (1), (2)", session=session)
        res = execute_sql_fetchall("select * from t order by a", session=session)
        assert res == [{"a": 1}, {"a": 2}]


def test_make_temp_table(session: Session):
    # error if inconsistent columns
    with pytest.raises(StatementError):
        make_temp_table(
            types={"a": "text", "b": "int"},
            data=[{"a": "a", "b": 1}, {"a": "a"}],
            pk="a",
            session=session,
        )

    # empty data
    tmp_name = make_temp_table(types={"a": "text", "b": "int"}, data=[], session=session)
    assert execute_sql_fetchall(f"select * from {tmp_name}", session=session) == []

    # correct entry
    tmp_name = make_temp_table(
        types={"a": "text", "b": "int"}, data=[{"a": "a", "b": 1}], session=session
    )
    assert execute_sql_fetchall(f"select * from {tmp_name}", session=session) == [
        {"a": "a", "b": 1}
    ]


def test_db_retry():
    """Test that db_retry works as expected."""

    class Namespace:
        """Namespace for patching."""

        @staticmethod
        def f():
            return 1

    # not retried bc invalid exc type
    with patch.object(Namespace, "f") as mock_f, pytest.raises(RuntimeError):
        mock_f.side_effect = [RuntimeError]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 1

    # ProgrammingError but not UndefinedTable
    with patch.object(Namespace, "f") as mock_f:
        mock_f.side_effect = [
            ProgrammingError("test", {}, orig=RuntimeError),
            1,
        ]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 2

    # retried once and then succeeded
    with patch.object(Namespace, "f") as mock_f:
        mock_f.side_effect = [
            ProgrammingError("test", {}, orig=UndefinedTable("test")),
            1,
        ]
        db_retry(Namespace.f)()
        assert mock_f.call_count == 2
