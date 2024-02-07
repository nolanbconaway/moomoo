"""Container classes for playlist data."""

from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from uuid import UUID


@dataclass(frozen=True)
class Track:
    """A candidate track in a playlist.

    Contains metadata about the track that can be used to construct a playlist.
    """

    filepath: Path
    recording_mbid: UUID | None = None
    release_mbid: UUID | None = None
    release_group_mbid: UUID | None = None
    artist_mbid: UUID | None = None
    album_artist_mbid: UUID | None = None
    distance: float | None = None

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

    def add_if_not_none(
        self, data: dict, key: str, type: Callable | None = None
    ) -> dict:
        """Append a key from this object to a dictionary if it is not None."""
        if hasattr(self, key) and getattr(self, key) is not None:
            value = getattr(self, key)
            if type is not None:
                value = type(value)
            return {**data, key: value}
        return data

    def to_dict(self, is_seed: bool | None = None) -> dict:
        """Convert to a dictionary, appropriate for json serialization."""
        res = {"filepath": str(self.filepath)}
        attrs = [
            "recording_mbid",
            "release_mbid",
            "release_group_mbid",
            "artist_mbid",
            "album_artist_mbid",
        ]
        for key in attrs:
            res = self.add_if_not_none(res, key, str)

        res = self.add_if_not_none(res, "distance")

        if is_seed is not None:
            res["seed"] = is_seed

        return res


@dataclass
class Playlist:
    """A full playlist.

    This contains an ordered list of tracks, as well as a title and description.
    """

    tracks: list[Track]
    title: str | None = None
    description: str | None = None

    def serialize_tracks(self) -> list[dict]:
        """Serialize the playlist list to a list of dicts, suitable for postgres."""
        return [track.to_dict() for track in self.tracks]
