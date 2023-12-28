"""Database module for moomoo_http."""
from typing import Optional

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from sqlalchemy.orm import Session

db = SQLAlchemy()


def execute_sql_fetchall(
    session: Session, sql: str, params: Optional[dict] = None
) -> list[dict]:
    """Execute a SQL statement and return all results via dict cursors."""
    res = session.execute(text(sql), params)
    if res.returns_rows:
        return list(map(dict, res.mappings()))
    return []
