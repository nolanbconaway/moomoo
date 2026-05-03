"""Connectivity utils for the database."""

import atexit
import os
from contextlib import suppress

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session


def get_engine() -> Engine:
    """Get a sqlalchemy engine for the db."""
    return create_engine(os.environ["MOOMOO_POSTGRES_URI"])


def get_session() -> Session:
    """Get a sqlalchemy session for the db.

    Automatically registers a close_session function to be called at exit.
    """
    session = Session(bind=get_engine())

    def close_session():
        with suppress(Exception):
            session.close()

    atexit.register(close_session)

    return session
