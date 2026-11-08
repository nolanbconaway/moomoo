"""Connectivity utils for the MusicBrainz database."""

import os

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from .db import _make_engine_from_uri


def get_engine() -> Engine:
    """Get a sqlalchemy engine for the MusicBrainz db."""
    return _make_engine_from_uri(os.environ["MUSICBRAINZ_POSTGRES_URI"])


def get_session() -> Session:
    """Get a sqlalchemy session for the MusicBrainz db."""
    return Session(bind=get_engine())


__all__ = ["get_engine", "get_session"]
