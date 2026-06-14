import base64
import datetime
import re
from typing import ClassVar, Literal
from uuid import UUID

import httpx
from moomoo_pg import PlaylistCollectionItem
from pydantic import BaseModel, ConfigDict, Field


class MoomooPlaylistSignature(BaseModel):
    """Defines the structure of the signature comment used to identify moomoo playlists."""

    moomoo_collection_id: UUID
    moomoo_playlist_id: UUID
    collection_name: str
    collection_order_index: int
    playlist_create_at: str
    signed_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    signature_prefix: ClassVar[str] = " --- moomoo playlist:"

    @classmethod
    def from_playlist(cls, playlist: PlaylistCollectionItem) -> "MoomooPlaylistSignature":
        """Creates a signature instance from a PlaylistCollection and a Playlist."""
        return cls(
            moomoo_collection_id=playlist.collection.collection_id,
            moomoo_playlist_id=playlist.playlist_id,
            collection_name=playlist.collection.collection_name,
            collection_order_index=playlist.collection_order_index,
            playlist_create_at=playlist.create_at_utc.isoformat(),
        )

    @classmethod
    def from_comment(cls, comment: str | None) -> "MoomooPlaylistSignature | None":
        """Parses a signature from a playlist comment, if present."""
        if not comment or cls.signature_prefix not in comment:
            return None

        pattern = rf"{re.escape(cls.signature_prefix)}\s*(?P<b64>[a-zA-Z0-9_-]+=*)"
        match = re.search(pattern, comment, re.MULTILINE)
        if not match:
            return None

        try:
            b64_part = match.group("b64")
            return cls.model_validate_json(base64.urlsafe_b64decode(b64_part))
        except Exception:
            return None

    @property
    def signature(self) -> str:
        """Encodes the signature data as a base64 JSON string with a prefix."""
        json_data = self.model_dump_json(exclude={"signature_prefix"})
        b64_str = base64.urlsafe_b64encode(json_data.encode()).decode()
        return f"{self.signature_prefix} {b64_str}"


class NavidromePlaylist(BaseModel):
    # https://opensubsonic.netlify.app/docs/endpoints/getplaylists/
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    class Entry(BaseModel):
        song_id: str = Field(alias="id")
        path: str
        title: str
        album: str
        artist: str

    playlist_id: str = Field(alias="id")
    name: str
    owner: str
    duration: int
    created: datetime.datetime
    changed: datetime.datetime
    songs: list[Entry] = Field(default_factory=list, alias="entry")
    comment: str | None = None

    @property
    def age(self) -> datetime.timedelta:
        """Returns the time elapsed since the playlist was last changed.

        Uses UTC now to ensure compatibility with Navidrome's ISO timestamps.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        return now - self.changed

    @property
    def signature(self) -> MoomooPlaylistSignature | None:
        """Attaches a moomoo signature to the playlist's comment field."""
        return MoomooPlaylistSignature.from_comment(self.comment)


class SubsonicError(BaseModel):
    code: int
    message: str


class SubsonicResponse(BaseModel):
    status: Literal["ok", "failed"]
    error: SubsonicError | None = None


class SubsonicStatusError(httpx.HTTPStatusError):
    """Raised when Subsonic returns 'failed' inside a 200 OK response."""

    def __init__(self, message, request, response, code):
        super().__init__(message, request=request, response=response)
        self.subsonic_code: int = code
