from moomoo_playlist.playlist import Playlist, Track


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


def test_Playlist__playlist():
    """Test that the playlist property works."""
    plist = Playlist(
        tracks=[Track(filepath=f"test/{i}") for i in range(10)],
        seeds=[Track(filepath="test/a")],
    )

    assert plist.playlist == [Track(filepath="test/a")] + [
        Track(filepath=f"test/{i}") for i in range(10)
    ]


def test_Playlist__shuffle():
    """Test that the shuffle method works."""
    plist = Playlist(
        tracks=[Track(filepath=f"test/{i}") for i in range(10)],
        seeds=[Track(filepath="test/a")],
    )
    plist.shuffle()

    assert plist.playlist[0] == Track(filepath="test/a")


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
