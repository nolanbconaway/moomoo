import pytest
from moomoo_pg import get_session
from sqlalchemy import text

from moomoo_ingest.musicbrainz.ddl import MusicBrainzBase


@pytest.fixture(autouse=True)
def create_musicbrainz_tables(mock_db):
    # require mock db as a dependency for this fixture, to make sure the base db is set up.
    with get_session() as session:
        session.execute(text("CREATE SCHEMA IF NOT EXISTS musicbrainz"))
        session.commit()
        MusicBrainzBase.metadata.create_all(session.get_bind())
