import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

from moomoo_pg import execute_sql_fetchall, get_session, make_temp_table


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
