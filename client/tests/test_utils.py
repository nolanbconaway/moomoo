"""Test the utils module."""

from pathlib import Path

import pytest
from moomoo_client.utils_ import MediaLibrary, Playlist


@pytest.fixture
def local_files(monkeypatch, tmp_path) -> Path:
    """Override the local files path and set it to a temporary directory."""
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(tmp_path))
    tmp_path = Path(tmp_path)
    (tmp_path / "test.mp3").touch()
    yield tmp_path


def test_MediaLibrary__location(monkeypatch):
    """Test the location property."""
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", "/tmp")
    library = MediaLibrary()
    assert library.location.exists()

    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", "/tmp/fakeeee")
    library = MediaLibrary()
    with pytest.raises(ValueError):
        # there is an exists check in the property
        library.location  # noqa: B018


def test_MediaLibrary__make_relative(local_files: Path):
    """Test the make_relative method."""
    library = MediaLibrary()
    assert library.make_relative(local_files / "test.mp3") == Path("test.mp3")

    # error if not in the library
    p = Path("/fake/test.mp3")
    with pytest.raises(ValueError):
        library.make_relative(p)


def test_MediaLibrary__make_absolute(local_files: Path):
    """Test the make_absolute method."""
    library = MediaLibrary()
    assert library.make_absolute("test.mp3") == local_files / "test.mp3"


def test_playlist__output_formats(local_files: Path):
    """Test the output formats."""
    fpath = local_files / "test.mp3"
    playlist = Playlist([fpath], description="aaa", generator="bbb")

    assert playlist.to_json() == '{"playlist": ["%s"], "description": "aaa"}' % fpath
    xml = playlist.to_xml()
    assert "<track>" in xml
    assert "<location>%s</location>" % fpath in xml
