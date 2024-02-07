"""Blueprint for the /playlist endpoint.

Use the base postgres connection in the playlist module for now. eventually should 
use a sqlalchemy session.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from flask import Blueprint, Response, request
from moomoo_playlist import FromFilesPlaylistGenerator, Playlist
from moomoo_playlist.ddl import PlaylistCollection

from ..db import db

base = Blueprint("playlist", __name__, url_prefix="/playlist")

# add a /playlist/suggest endpoint for the suggested playlist generator
suggest = Blueprint("suggest", __name__, url_prefix="/suggest")
base.register_blueprint(suggest)


def boolean_type(v: str) -> bool:
    """Convert a string to a boolean."""
    return v.lower() in ["1", "true"]


@dataclass
class PlaylistResponse:
    """Dataclass for a playlist response."""

    success: bool
    playlists: list[Playlist] | None = None
    error: str | None = None

    def __post__init__(self):
        if self.playlists is not None and self.error is not None:
            raise ValueError("Cannot have both playlists and error.")

    @staticmethod
    def serialize_playlist(playlist: Playlist) -> dict:
        """Serialize a playlist."""
        res = {"playlist": playlist.serialize_tracks()}
        if playlist.title is not None:
            res["title"] = playlist.title
        if playlist.description is not None:
            res["description"] = playlist.description
        return res

    def to_serializable(self) -> dict:
        """Convert to a dictionary, appropriate for json serialization."""
        res = {"success": self.success}

        if self.playlists is not None:
            res["playlists"] = [self.serialize_playlist(p) for p in self.playlists]

        if self.error is not None:
            res["error"] = self.error

        return res

    def to_http(self, status_code: int | None = None) -> Response:
        """Convert to a flask response."""
        if status_code is None:
            status_code = 200 if self.success else 500

        return Response(
            json.dumps(self.to_serializable()),
            status=status_code,
            content_type="application/json",
        )


@base.route("/from-files", methods=["GET"])
def from_files():
    """Create a playlist from one or more files."""
    n_tracks = request.args.get("n", 20, type=int)
    seed = request.args.get("seed", 1, type=int)
    paths = request.args.getlist("path", type=Path)

    if len(paths) == 0:
        return PlaylistResponse(success=False, error="No paths provided.").to_http(400)

    generator = FromFilesPlaylistGenerator(*paths)

    try:
        playlist = generator.get_playlist(
            limit=n_tracks, shuffle=True, seed_count=seed, session=db.session
        )
    except Exception as e:
        return PlaylistResponse(
            success=False, error=f"{type(e).__name__}: {e}"
        ).to_http()

    return PlaylistResponse(success=True, playlists=[playlist]).to_http()


@base.route("/loved/<username>", methods=["GET"])
def loved_tracks(username: str):
    """Make a playlist of loved tracks for a user."""
    collection_name = "loved-tracks"
    collection = (
        db.session.query(PlaylistCollection)
        .filter_by(username=username, collection_name=collection_name)
        .first()
    )
    if collection is None:
        return PlaylistResponse(
            success=False,
            error=f"Collection {collection_name} collection not found for {username}.",
        ).to_http()
    if not collection.playlists:
        return PlaylistResponse(
            success=False, error=f"No {collection_name} playlists found for {username}."
        ).to_http()

    playlist = collection.playlists[0].to_playlist()
    return PlaylistResponse(success=True, playlists=[playlist]).to_http()


@base.route("/revisit-releases/<username>", methods=["GET"])
def revisit_releases(username: str):
    """Generate playlists of releases to revisit for a user."""
    collection_name = "revisit-releases"
    collection = (
        db.session.query(PlaylistCollection)
        .filter_by(username=username, collection_name=collection_name)
        .first()
    )

    if collection is None:
        return PlaylistResponse(
            success=False,
            error=f"Collection {collection_name} collection not found for {username}.",
        ).to_http()
    if not collection.playlists:
        return PlaylistResponse(
            success=False, error=f"No {collection_name} playlists found for {username}."
        ).to_http()

    playlists = [item.to_playlist() for item in collection.playlists]
    return PlaylistResponse(success=True, playlists=playlists).to_http()


@suggest.route("/by-artist/<username>", methods=["GET"])
def suggest_by_artist(username: str):
    """Suggest playlist based on most listened to artists."""
    collection_name = "top-artists"
    collection = (
        db.session.query(PlaylistCollection)
        .filter_by(username=username, collection_name=collection_name)
        .first()
    )

    if collection is None:
        return PlaylistResponse(
            success=False,
            error=f"Collection {collection_name} collection not found for {username}.",
        ).to_http()
    if not collection.playlists:
        return PlaylistResponse(
            success=False, error=f"No {collection_name} playlists found for {username}."
        ).to_http()

    playlists = [item.to_playlist() for item in collection.playlists]
    return PlaylistResponse(success=True, playlists=playlists).to_http()
