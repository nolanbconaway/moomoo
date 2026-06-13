"""Connectivity utils for the database."""

import datetime
import json
import os
from uuid import UUID, uuid4

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session


class JSONSerializer(json.JSONEncoder):
    """JSON serializer for UUID."""

    def default(self, obj):
        """Serialize UUID to hex, else default to json serializer."""
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def json_dumps(*args, **kw):
    """Dump a json object with custom serializer."""
    return json.dumps(*args, cls=JSONSerializer, **kw)


def get_engine() -> Engine:
    """Get a sqlalchemy engine for the db."""
    uri = os.environ["MOOMOO_POSTGRES_URI"]
    return create_engine(uri, json_serializer=json_dumps)


def get_session() -> Session:
    """Get a sqlalchemy session for the db."""
    return Session(bind=get_engine())


def execute_sql_fetchall(
    sql: str, params: dict | None = None, session: Session = None
) -> list[dict]:
    """Execute a SQL statement and return all results via dict cursors."""

    def f(s: Session):
        res = s.execute(text(sql), params)
        if res.returns_rows:
            return list(map(dict, res.mappings()))
        return []

    if session is None:
        with get_session() as session:
            return f(session)

    return f(session)


def make_tmp_name() -> str:
    """Make a temporary table name.

    Follows the pattern tmp_{datetime}_{uuid}.
    """
    uuid = str(uuid4())[-12:].replace("-", "")
    dt = datetime.datetime.now().strftime("%Y%m%d")
    return f"tmp_{dt}_{uuid}"


def make_temp_table(
    types: dict[str, str], data: list[dict], session: Session, pk: str | None = None
) -> str:
    """Make a temporary table from data, returning the table name.

    Args:
        types: Dictionary of column names to types.
        data: List of dictionaries to insert into the table.
        session: Sqlalchemy session.
        pk: Primary key column name.

    Returns:
        The name of the temporary table.
    """
    tmp_name = make_tmp_name()
    columns = []
    for k, v in types.items():
        if k == pk:
            columns.append(f"{k} {v} primary key")
        else:
            columns.append(f"{k} {v}")

    session.execute(text(f"""create temp table {tmp_name} ({", ".join(columns)})"""))

    if data:
        cols = ", ".join(types.keys())
        values = ", ".join([f":{k}" for k in types])
        sql = f"insert into {tmp_name} ({cols}) values ({values})"
        session.execute(text(sql), data)

    return tmp_name


__all__ = [
    "execute_sql_fetchall",
    "get_engine",
    "get_session",
    "make_temp_table",
]
