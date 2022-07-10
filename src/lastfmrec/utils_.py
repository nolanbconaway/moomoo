import datetime
import hashlib
import json
import os
from typing import List
import sys
import time

import psycopg2
import requests


DDL = [
    """
    create table {schema}.{table} (
        listen_md5 varchar(32) not null primary key
        , username text not null
        , json_data jsonb not null
        , listen_at_ts_utc timestamp with time zone not null
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
    """create index {schema}_{table}_username_idx on {schema}.{table} (username)""",
    """create index {schema}_{table}_listen_at_idx on {schema}.{table} (listen_at_ts_utc)""",
]


def pg_connect(dsn: str = None) -> psycopg2.extensions.connection:
    """Connect to the db."""
    return psycopg2.connect(dsn or os.environ["POSTGRES_DSN"])


def get_with_retries(
    *args, retries: int = 3, delay: float = 3, timeout_: float = 2, **kwargs
) -> requests.Response:
    for i in range(retries):
        try:
            res = requests.get(*args, **kwargs)
            time.sleep(timeout_)
            res.raise_for_status()
            return res
        except requests.exceptions.HTTPError:
            if i == retries - 1:
                raise
            else:
                print("Connection error, retrying in {} seconds".format(delay))
                time.sleep(delay)


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
