import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

from moomoo_playlist.db import execute_sql_fetchall, make_temp_table


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
