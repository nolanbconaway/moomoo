from moomoo_playlist.collections.top_artists import list_top_artists
from moomoo_playlist.collections.top_artists import main as top_artists_main
from sqlalchemy.orm import Session


def test_list_top_artists__count(session: Session):
    raise NotImplementedError()


def test_list_top_artists__history_length(session: Session):
    raise NotImplementedError()


def test_list_top_artists__no_results(session: Session):
    raise NotImplementedError()


def test_main__no_results(session: Session):
    raise NotImplementedError()


def test_main__playlist_error(session: Session):
    raise NotImplementedError()


def test_main__storage(session: Session):
    raise NotImplementedError()
