"""Blueprint for the /playlist endpoint.

Use the base postgres connection in the playlist module for now. eventually should 
use a sqlalchemy session.
"""
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path

from flask import Blueprint, Request, request

from ..playlist import PlaylistGenerator
from .logger import get_logger

logger = get_logger(__name__)


bp = Blueprint("playlist", __name__, url_prefix="/playlist")


def dbt_schema() -> str:
    """Get the dbt schema."""
    return os.environ["MOOMOO_DBT_SCHEMA"]


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
            shuffle=request.args.get(
                "shuffle", "1", type=lambda v: v.lower() in ["1", "true"]
            ),
        )


@bp.route("/from-files", methods=["GET"])
def from_files():
    """Create a playlist from one or more files."""
    args = PlaylistArgs.from_request(request)
    paths = request.args.getlist("path", type=Path)

    logger.info(f"from-files request: {paths} ({args})")

    if len(paths) == 0:
        return ({"success": False, "error": "No filepaths provided."}, 400)
    elif len(paths) > 500:
        return {"success": False, "error": "Too many filepaths provided (>500)."}, 400

    try:
        generator = PlaylistGenerator.from_files(paths, schema=dbt_schema())
        plist = generator.get_playlist(
            schema=dbt_schema(),
            limit=args.n,
            shuffle=args.shuffle,
            seed_count=args.seed,
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return ({"success": False, "error": f"{type(e).__name__}: {e}"}, 500)

    return {
        "success": True,
        "playlist": [str(f) for f in plist.playlist],
        "source_paths": [str(f) for f in plist.source_paths],
    }


@bp.route("/from-parent-path", methods=["GET"])
def from_parent_path():
    """Create a playlist from a parent path."""
    args = PlaylistArgs.from_request(request)
    path = request.args.get("path", type=Path)

    logger.info(f"from-parent request: {path} ({args})")
    if not path:
        return ({"success": False, "error": "No path provided."}, 400)

    try:
        generator = PlaylistGenerator.from_parent_path(path, schema=dbt_schema())
        plist = generator.get_playlist(
            schema=dbt_schema(),
            limit=args.n,
            shuffle=args.shuffle,
            seed_count=args.seed,
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return ({"success": False, "error": f"{type(e).__name__}: {e}"}, 500)

    return {
        "success": True,
        "playlist": [str(f) for f in plist.playlist],
        "source_paths": [str(f) for f in plist.source_paths],
    }
