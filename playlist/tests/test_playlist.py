from pathlib import Path
from uuid import UUID

from moomoo_playlist.playlist import Playlist, Track


def test_track__track_type_casting():
    """Test that the Track class casts the filepath to a Path object."""
    uuid = "00000000-0000-0000-0000-000000000000"
    t = Track(filepath="test/test.mp3", recording_mbid=uuid)
    assert isinstance(t.filepath, Path)
    assert t.filepath == Path("test/test.mp3")
    assert isinstance(t.recording_mbid, UUID)
    assert t.recording_mbid == UUID(uuid)


def test_Track__to_dict():
    t = Track(filepath="test/test.mp3")
    assert t.to_dict() == {"filepath": "test/test.mp3"}

    t = Track(
        filepath="test/test.mp3",
        recording_mbid="00000000-0000-0000-0000-000000000000",
        release_mbid="00000000-0000-0000-0000-000000000000",
        distance=1,
    )
    assert t.to_dict() == {
        "filepath": "test/test.mp3",
        "recording_mbid": "00000000-0000-0000-0000-000000000000",
        "release_mbid": "00000000-0000-0000-0000-000000000000",
        "distance": 1,
    }


def test_Playlist__setter_getter():
    """Test that the title and description properties work."""
    plist = Playlist(
        tracks=[Track(filepath=f"test/{i}") for i in range(10)],
    )
    assert plist.title is None
    assert plist.description is None

    plist.title = "test title"
    plist.description = "test description"
    assert plist.title == "test title"
    assert plist.description == "test description"


def test_playlist__track_type_casting():
    """Test that the tracks are cast to Track objects."""
    plist = Playlist(
        tracks=[{"filepath": f"test/{i}"} for i in range(10)],
    )
    assert all(isinstance(track, Track) for track in plist.tracks)
    assert all(isinstance(track.filepath, Path) for track in plist.tracks)
