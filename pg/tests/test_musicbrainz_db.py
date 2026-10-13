from sqlalchemy.orm import Session

from moomoo_pg import get_musicbrainz_session


def test_get_session():
    """Test that get_musicbrainz_session returns a valid session."""
    session = get_musicbrainz_session()
    assert isinstance(session, Session)
