"""Connectivity utils for the database."""
import json
import os
from uuid import UUID

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
    ingest_schema = os.environ["MOOMOO_INGEST_SCHEMA"]
    return create_engine(
        os.environ["MOOMOO_POSTGRES_URI"],
        json_serializer=json_dumps,
        connect_args=dict(options=f"-c search_path={ingest_schema}"),
    )


def get_session() -> Session:
    """Get a sqlalchemy session for the db."""
    return Session(bind=get_engine())


def execute_sql_fetchall(
    sql: str, params: dict = None, session: Session = None
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


def check_table_exists(schema: str, table: str) -> bool:
    """Check if a table exists in the db."""
    sql = """
        select 1
        from information_schema.tables 
        where table_schema = :schema
        and table_name = :table
        limit 1
    """
    return any(execute_sql_fetchall(sql, params=dict(schema=schema, table=table)))
