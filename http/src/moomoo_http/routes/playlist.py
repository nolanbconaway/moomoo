"""Blueprint for the /playlist endpoint.

Use the base postgres connection in the playlist module for now. eventually should 
use a sqlalchemy session.
"""
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from flask import Blueprint, Request, request

from ..db import MoomooPlaylist, db
from ..playlist_generator import (
    BasePlaylistGenerator,
    FromFilesPlaylistGenerator,
    FromMbidsPlaylistGenerator,
)
from .logger import get_logger

logger = get_logger(__name__)


bp = Blueprint("playlist", __name__, url_prefix="/playlist")


def boolean_type(v: str) -> bool:
    """Convert a string to a boolean."""
    return v.lower() in ["1", "true"]


@dataclass
class PlaylistArgs:
    """Dataclass for common playlist arguments."""

    n: int
    seed: int
    shuffle: bool

    @classmethod
    def from_request(cls, request: Request) -> "PlaylistArgs":
        """Create a PlaylistArgs object from a request."""
        return cls(
            n=request.args.get("n", 20, type=int),
            seed=request.args.get("seed", 0, type=int),
            shuffle=request.args.get("shuffle", "1", type=boolean_type),
        )


def get_playlist_result(
    generator: BasePlaylistGenerator, args: PlaylistArgs, username: str
) -> dict:
    """Get a playlist from a generator.

    Returns the result as needed for http, and inserts the playlist record into the
    database.
    """
    logger.info(f"playlist request: {generator.name} / {username} / ({args})")

    try:
        plist_paths, source_paths = generator.get_playlist(
            limit=args.n,
            shuffle=args.shuffle,
            seed_count=args.seed,
            session=db.session,
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return ({"success": False, "error": f"{type(e).__name__}: {e}"}, 500)

    plist_strs = list(map(str, plist_paths))
    source_strs = list(map(str, source_paths))

    # try to insert the playlist into the database, but don't raise if it fails
    db_plist = MoomooPlaylist(
        username=username,
        generator=generator.name,
        playlist=plist_strs,
        source_paths=source_strs,
    )

    try:
        db.session.add(db_plist)
        db.session.commit()
        logger.info("Inserted playlist.")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        logger.error(f"Failed to insert playlist: {type(e).__name__}: {e}")

    return {"success": True, "playlist": plist_strs, "source_paths": source_strs}


@bp.route("/from-files", methods=["GET"])
def from_files():
    """Create a playlist from one or more files."""
    args = PlaylistArgs.from_request(request)
    paths = request.args.getlist("path", type=Path)
    username = request.headers.get("listenbrainz-username")

    if username is None:
        return (
            {"success": False, "error": "No listenbrainz-username header provided."},
            400,
        )
    if len(paths) == 0:
        return ({"success": False, "error": "No filepaths provided."}, 400)
    elif len(paths) > 500:
        return {"success": False, "error": "Too many filepaths provided (>500)."}, 400

    generator = FromFilesPlaylistGenerator(*paths)
    return get_playlist_result(generator, args, username)


@bp.route("/from-mbids", methods=["GET"])
def from_mbids():
    """Create a playlist from one or more mbids."""
    args = PlaylistArgs.from_request(request)
    mbids = request.args.getlist("mbid", type=str)
    username = request.headers.get("listenbrainz-username")

    if username is None:
        return (
            {"success": False, "error": "No listenbrainz-username header provided."},
            400,
        )

    if len(mbids) == 0:
        return ({"success": False, "error": "No mbids provided."}, 400)
    elif len(mbids) > 500:
        return {"success": False, "error": "Too many mbids provided (>500)."}, 400
    else:
        # try casting to UUID, and return 400 if any fail
        try:
            mbids = [UUID(hex=mbid) for mbid in mbids]
        except ValueError:
            return ({"success": False, "error": "Invalid mbid format provided."}, 400)

    generator = FromMbidsPlaylistGenerator(*mbids)
    return get_playlist_result(generator, args, username)
