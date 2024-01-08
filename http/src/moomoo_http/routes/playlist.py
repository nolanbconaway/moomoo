"""Blueprint for the /playlist endpoint.

Use the base postgres connection in the playlist module for now. eventually should 
use a sqlalchemy session.
"""
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from uuid import UUID

from flask import Blueprint, Request, Response, request

from ..db import db, execute_sql_fetchall
from ..playlist_generator import (
    BasePlaylistGenerator,
    FromFilesPlaylistGenerator,
    FromMbidsPlaylistGenerator,
    Playlist,
    QueryPlaylistGenerator,
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
            shuffle=request.args.get("shuffle", True, type=boolean_type),
        )


def make_http_response(
    generators: list[BasePlaylistGenerator], args: PlaylistArgs
) -> Response:
    """Generate a http response from a list of generators and args."""
    if not generators:
        raise ValueError("No generators provided.")

    @dataclass
    class Result:
        generator: BasePlaylistGenerator
        playlist: Optional[Playlist] = None
        error: Optional[Exception] = None

    # run all generators, catching any errors. results will be a list of Result objects
    # TODO: make this async/parallel?? something faster
    results, n = [], len(generators)
    for i, generator in enumerate(generators, 1):
        try:
            logger.info(
                f"Getting playlist {i}/{n}. generator=%s, description=%s, args=%s",
                generator.name,
                generator.description,
                args,
            )
            plist = generator.get_playlist(
                limit=args.n,
                shuffle=args.shuffle,
                seed_count=args.seed,
                session=db.session,
            )
            results.append(Result(generator=generator, playlist=plist))
        except Exception as e:
            logger.exception(f"Error getting playlist {i}/{n}")
            results.append(Result(generator=generator, error=e))

    # grab the successful playlists and errors
    success = list(r.playlist for r in results if r.playlist is not None)
    errors = list(r.error for r in results if r.error is not None)

    if not success:
        e = errors[0]
        return Response(
            json.dumps({"success": False, "error": f"{type(e).__name__}: {e}"}),
            status=500,
            content_type="application/json",
        )

    # otherwise return a list of playlists
    return Response(
        json.dumps({"success": True, "playlists": [p.to_dict() for p in success]}),
        status=200,
        content_type="application/json",
    )


@base.route("/from-files", methods=["GET"])
def from_files():
    """Create a playlist from one or more files."""
    args = PlaylistArgs.from_request(request)
    paths = request.args.getlist("path", type=Path)

    if len(paths) == 0:
        return ({"success": False, "error": "No filepaths provided."}, 400)
    elif len(paths) > 500:
        return {"success": False, "error": "Too many filepaths provided (>500)."}, 400

    generator = FromFilesPlaylistGenerator(*paths)
    return make_http_response([generator], args)


@base.route("/from-mbids", methods=["GET"])
def from_mbids():
    """Create a playlist from one or more mbids."""
    args = PlaylistArgs.from_request(request)
    mbids = request.args.getlist("mbid", type=str)

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
    return make_http_response([generator], args)


@base.route("/loved/<username>", methods=["GET"])
def loved_tracks(username: str):
    """Make a playlist of loved tracks for a user."""
    args = PlaylistArgs.from_request(request)  # note: not used here at all
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select filepath
        from {schema}.loved_tracks
        where username = :username
        order by love_at desc
    """
    generator = QueryPlaylistGenerator(
        sql=sql,
        params=dict(username=username),
        description=f"Loved tracks for {username}",
    )
    return make_http_response([generator], args)


@base.route("/revisit-releases/<username>", methods=["GET"])
def revisit_releases(username: str):
    """Generate playlists of releases to revisit for a user."""
    args = PlaylistArgs.from_request(request)  # note: not used here at all
    count_plists = request.args.get("numPlaylists", 5, type=int)
    schema = os.environ["MOOMOO_DBT_SCHEMA"]

    # get release groups for the user
    sql = f"""
        select release_group_mbid, release_group_title, artist_name
        from {schema}.revisit_releases
        where username = :username
        order by random()
        limit :n
    """
    groups = execute_sql_fetchall(
        session=db.session, sql=sql, params=dict(username=username, n=count_plists)
    )

    if not groups:
        return ({"success": False, "error": "No revisit releases found."}, 500)

    groups = sorted(groups, key=lambda x: (x["artist_name"], x["release_group_title"]))

    # make a generator for each release group
    sql = f"""
        select filepath
        from {schema}.map__file_release_group
        where release_group_mbid=:mbid
        order by filepath
    """

    generators = [
        QueryPlaylistGenerator(
            sql=sql,
            params=dict(mbid=row["release_group_mbid"]),
            description=f"Revisit: {row['release_group_title']} - {row['artist_name']}",
        )
        for row in groups
    ]

    return make_http_response(generators, args)


@suggest.route("/by-artist/<username>", methods=["GET"])
def suggest_by_artist(username: str):
    """Suggest playlist based on most listened to artists."""
    args = PlaylistArgs.from_request(request)
    count_plists = request.args.get("numPlaylists", 4, type=int)
    history_days = request.args.get("historyDays", "90")

    history_column_map = {
        "30": "last30_listen_count",
        "60": "last60_listen_count",
        "90": "last90_listen_count",
        "lifetime": "lifetime_listen_count",
    }

    if history_days not in history_column_map:
        values = list(history_column_map.keys())
        return (
            {
                "success": False,
                "error": f"Invalid historyDays value. Must be one of {values}",
            },
            400,
        )

    if count_plists < 1:
        return ({"success": False, "error": "numPlaylists must be >= 1."}, 400)

    # get the top n artists from last 30 days with more than 10 listens
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    history_column = history_column_map[history_days]
    sql = f"""
        select artist_mbid, artist_name
        from {schema}.artist_listen_counts
        where username = :username and {history_column} > 10
        order by {history_column} desc
        limit :n
    """
    rows = execute_sql_fetchall(
        session=db.session, sql=sql, params=dict(username=username, n=count_plists)
    )

    if not rows:
        return ({"success": False, "error": "No artists found for user."}, 500)

    # get responses for each artist
    generators = [
        FromMbidsPlaylistGenerator(
            row["artist_mbid"], description=f"Artist: {row['artist_name']}"
        )
        for row in rows
    ]

    return make_http_response(generators, args)
