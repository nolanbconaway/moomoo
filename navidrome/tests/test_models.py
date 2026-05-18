import base64
import datetime
from unittest.mock import Mock
from uuid import UUID, uuid1

import pytest

from moomoo_navidrome.models import MoomooPlaylistSignature


@pytest.fixture
def sample_signature():
    """Create a sample signature for testing."""
    return MoomooPlaylistSignature(
        moomoo_collection_id=uuid1(),
        moomoo_playlist_id=uuid1(),
        collection_name="Test Collection",
        collection_order_index=0,
        playlist_create_at="2026-05-17T12:00:00",
    )


def test_from_comment_valid_signature(sample_signature):
    """Test from_comment parses valid signature from comment text."""
    comment = f"Some comment text{sample_signature.signature}"

    parsed = MoomooPlaylistSignature.from_comment(comment)
    assert parsed is not None
    assert parsed.moomoo_collection_id == sample_signature.moomoo_collection_id
    assert parsed.moomoo_playlist_id == sample_signature.moomoo_playlist_id
    assert parsed.collection_name == sample_signature.collection_name
    assert parsed.collection_order_index == sample_signature.collection_order_index


@pytest.mark.parametrize("comment", ["Just a regular comment", None])
def test_from_comment_no_signature(comment):
    """Test from_comment returns None for comment without signature."""
    assert MoomooPlaylistSignature.from_comment(comment) is None


def test_from_comment_invalid_base64():
    """Test from_comment returns None for invalid base64."""
    comment = f"Comment {MoomooPlaylistSignature.signature_prefix} !!invalid!!"
    assert MoomooPlaylistSignature.from_comment(comment) is None


def test_signature_property_encoding(sample_signature):
    """Test signature property encodes to base64 with prefix."""
    encoded = sample_signature.signature
    assert encoded.startswith(MoomooPlaylistSignature.signature_prefix)

    # Verify it can be decoded
    b64_part = encoded.split(MoomooPlaylistSignature.signature_prefix)[1].strip()
    decoded = base64.urlsafe_b64decode(b64_part)
    assert decoded is not None
