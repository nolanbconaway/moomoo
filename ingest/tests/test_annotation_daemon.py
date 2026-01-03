"""Test the annotation daemon.

Tests are very light right now, as its just a thin wrapper around the annotate_mbids module which
is well-tested.
"""

import uuid

import pytest

from moomoo_ingest import annotate_mbids, annotation_daemon
from moomoo_ingest.db import MusicBrainzAnnotation


@pytest.fixture(autouse=True)
def create_tables():
    """Create and drop the necessary tables for testing."""
    MusicBrainzAnnotation.create()


def test_run(monkeypatch):
    """Test the ingest batch function."""
    # add some mbids to each category to fetch
    monkeypatch.setattr(
        annotate_mbids,
        "get_unannotated_mbids",
        lambda: [dict(mbid=uuid.uuid4(), entity="recording") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_updated_mbids",
        lambda: [dict(mbid=uuid.uuid4(), entity="artist") for _ in range(10)],
    )
    monkeypatch.setattr(
        annotate_mbids,
        "get_very_old_annotations",
        lambda _: [dict(mbid=uuid.uuid4(), entity="release") for _ in range(10)],
    )

    # nothing to do
    assert (
        annotation_daemon.run(new_=False, updated=False, reannotate_after_days=0, batch_size=100)
        == 0
    )

    # run everything
    n = annotation_daemon.run(new_=True, updated=True, reannotate_after_days=1, batch_size=100)
    assert n == 30
