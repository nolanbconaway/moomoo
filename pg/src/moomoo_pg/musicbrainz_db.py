"""Connectivity utils for the MusicBrainz database."""

import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from .db import json_dumps


def get_engine() -> Engine:
    """Get a sqlalchemy engine for the MusicBrainz db."""
    uri = os.environ["MUSICBRAINZ_POSTGRES_URI"]
    return create_engine(uri, json_serializer=json_dumps)


def get_session() -> Session:
    """Get a sqlalchemy session for the MusicBrainz db."""
    return Session(bind=get_engine())


__all__ = ["get_engine", "get_session"]
