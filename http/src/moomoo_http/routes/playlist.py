"""Blueprint for the /playlist endpoint.

Use the base postgres connection in the playlist module for now. eventually should 
use a sqlalchemy session.
"""
import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from flask import Blueprint, Request, Response, request

from ..db import db, execute_sql_fetchall
from ..playlist_generator import (
    BasePlaylistGenerator,
    FromFilesPlaylistGenerator,
    FromMbidsPlaylistGenerator,
)
from .logger import get_logger

logger = get_logger(__name__)


base = Blueprint("playlist", __name__, url_prefix="/playlist")

# add a /playlist/suggest endpoint for the suggested playlist generator
suggest = Blueprint("suggest", __name__, url_prefix="/suggest")
base.register_blueprint(suggest)


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


def single_playlist_response(
    generator: BasePlaylistGenerator, args: PlaylistArgs
) -> Response:
    """Get an http response with a single playlist from a generator.

    Wraps the logic in a try except block to catch errors and return a 500 response.
    """
    logger.info(f"playlist request: {generator.name} / ({args})")

    try:
        playlist = generator.get_playlist(
            limit=args.n, shuffle=args.shuffle, seed_count=args.seed, session=db.session
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return Response(
            json.dumps({"success": False, "error": f"{type(e).__name__}: {e}"}),
            status=500,
            content_type="application/json",
        )

    return Response(
        json.dumps({"success": True, **playlist.to_dict()}),
        status=200,
        content_type="application/json",
    )


def multi_playlist_response(
    generators: list[BasePlaylistGenerator], args: PlaylistArgs
) -> Response:
    """Produce a response with multiple playlists.

    Wraps the logic in a try except block to catch errors and return a 500 response.
    Uses the first error message if no playlists are generated, else skips along and
    returns the playlists that were generated.
    """
    n, playlists, errors = len(generators), [], []

    for i, generator in enumerate(generators):
        try:
            logger.info(f"Getting playlist {i+1}/{n}: {generator.name} / ({args})")
            plist = generator.get_playlist(
                limit=args.n,
                shuffle=args.shuffle,
                seed_count=args.seed,
                session=db.session,
            )
            playlists.append(plist)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            errors.append(e)

    if not playlists:
        e = errors[0]
        return Response(
            json.dumps({"success": False, "error": f"{type(e).__name__}: {e}"}),
            status=500,
            content_type="application/json",
        )

    if errors:
        logger.warning(f"Some playlists failed: {errors}")

    return Response(
        json.dumps({"success": True, "playlists": [p.to_dict() for p in playlists]}),
        status=200,
        content_type="application/json",
    )


@base.route("/from-files", methods=["GET"])
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
    return single_playlist_response(generator, args)


@base.route("/from-mbids", methods=["GET"])
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
    return single_playlist_response(generator, args)


@suggest.route("/by-artist", methods=["GET"])
def suggest_by_artist():
    """Suggest playlist based on most listened to artists."""
    args = PlaylistArgs.from_request(request)
    username = request.headers.get("listenbrainz-username")
    count_plists = request.args.get("numPlaylists", 5, type=int)

    if username is None:
        return (
            {"success": False, "error": "No listenbrainz-username header provided."},
            400,
        )

    logger.info(f"playlist request: by-artist / {username} / ({args})")

    # get the top n artists from last 30 days with more than 10 listens
    sql = """
        select artist_mbid, artist_name
        from moomoo.artist_listen_counts
        where username = :username
          and last30_listen_count > 10
        order by last30_listen_count desc
        limit :n
    """
    rows = execute_sql_fetchall(
        session=db.session, sql=sql, params=dict(username=username, n=count_plists)
    )

    if not rows:
        return ({"success": False, "error": "No artists found."}, 500)

    # get responses for each artist
    generators = [
        FromMbidsPlaylistGenerator(
            row["artist_mbid"], description=f"Artist: {row['artist_name']}"
        )
        for row in rows
    ]

    return multi_playlist_response(generators, args)
