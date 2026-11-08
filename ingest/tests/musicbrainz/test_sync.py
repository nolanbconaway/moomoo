from uuid import uuid1

from moomoo_pg import get_session

from moomoo_ingest.musicbrainz import sync
from moomoo_ingest.musicbrainz.ddl import MusicBrainzBase
from moomoo_ingest.musicbrainz.mbids import Mbid


class FakeManager:
    stg_schema = "staging"
    promoted = False

    def __init__(self, *_):
        pass

    @staticmethod
    def drop_orphaned_schemas(*_):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def promote(self):
        type(self).promoted = True


def test_run_copy(monkeypatch):
    with get_session() as app_session, get_session() as mbz_session:
        monkeypatch.setattr(sync, "get_musicbrainz_session", lambda: mbz_session)
        monkeypatch.setattr(sync, "get_session", lambda: app_session)
        monkeypatch.setattr(
            sync,
            "get_mbids",
            lambda *_, **__: [Mbid(mbid=uuid1(), entity="artist")],
        )
        monkeypatch.setattr(MusicBrainzBase, "copy", classmethod(lambda cls, *_, **__: None))
        monkeypatch.setattr(sync, "StagingSchemaManager", FakeManager)
        FakeManager.promoted = False

        sync.run_copy(batch_size=1)

        assert FakeManager.promoted
