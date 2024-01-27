"""Container classes for playlist data."""
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from uuid import UUID


def str_or_none(val: Optional[str]) -> Optional[str]:
    """Convert a string to None if it is empty."""
    return None if val is None or val == "" else str(val)


@dataclass(frozen=True)
class Track:
    """A candidate track in a playlist.

    Contains metadata about the track that can be used to construct a playlist.
    """

    filepath: Path
    recording_mbid: Optional[UUID] = None
    release_mbid: Optional[UUID] = None
    release_group_mbid: Optional[UUID] = None
    artist_mbid: Optional[UUID] = None
    album_artist_mbid: Optional[UUID] = None
    distance: Optional[float] = None

    def __post__init__(self):
        # cast mbids to UUIDs if they are strings
        attrs = [
            "recording_mbid",
            "release_mbid",
            "release_group_mbid",
            "artist_mbid",
            "album_artist_mbid",
        ]
        for attr in attrs:
            val = getattr(self, attr)
            if val is not None and isinstance(val, str):
                setattr(self, attr, UUID(val))

        # cast filepath to Path if it is a string
        if isinstance(self.filepath, str):
            self.filepath = Path(self.filepath)

    def add_if_not_none(self, data: dict, key: str) -> dict:
        """Append a key from this object to a dictionary if it is not None."""
        if hasattr(self, key) and getattr(self, key) is not None:
            return {**data, key: getattr(self, key)}
        return data

    def to_dict(self) -> dict:
        """Convert to a dictionary, appropriate for json serialization."""
        res = {"filepath": str(self.filepath)}
        attrs = [
            "recording_mbid",
            "release_mbid",
            "release_group_mbid",
            "artist_mbid",
            "album_artist_mbid",
            "distance",
        ]
        for key in attrs:
            res = self.add_if_not_none(res, key)

        return res


@dataclass
class Playlist:
    """A full playlist.

    This object should contain all that is needed to populate client-side playlist.
    """

    tracks: list[Track]
    seeds: list[Track] = field(default_factory=list)
    title: Optional[str] = None
    description: Optional[str] = None

    def shuffle(self) -> "Playlist":
        """Shuffle the playlist inplace."""
        random.shuffle(self.tracks)
        return self

    @property
    def playlist(self) -> list[Track]:
        """Get the playlist as a list of tracks."""
        return self.seeds + self.tracks

    def serialize_list(self) -> list[dict]:
        """Serialize the playlist list to a list of dicts, suitable for postgres."""
        return [track.to_dict() for track in self.playlist]
