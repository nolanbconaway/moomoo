import datetime
import os
from typing import List

import psycopg2


def pg_connect(dsn: str = None) -> psycopg2.extensions.connection:
    """Connect to the db."""
    return psycopg2.connect(dsn or os.environ["POSTGRES_DSN"])


def create_table(schema: str, table: str, ddl: List[str]):
    print('Creating table "{}" in schema "{}"...'.format(table, schema))
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"drop table if exists {schema}.{table}".format(
                    schema=schema, table=table
                )
            )
            for sql in ddl:
                cur.execute(sql.format(schema=schema, table=table))
        conn.commit()


def check_table_exists(schema: str, table: str) -> bool:
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select 1
                from information_schema.tables 
                where table_schema = %(schema)s
                and table_name = %(table)s
                limit 1
                """,
                dict(schema=schema, table=table),
            )
            res = cur.fetchall()
    return any(res)


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY_MM_DD date to UTC datetime."""
    return datetime.datetime.fromisoformat(iso_date).replace(
        tzinfo=datetime.timezone.utc
    )


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )
