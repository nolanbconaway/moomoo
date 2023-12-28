"""Application metadata views (ping, version, etc)"""
from pathlib import Path

from flask import Blueprint

VERSION_FILE = Path(__file__).parent.parent / "version"

base = Blueprint("app_meta", __name__, url_prefix="")


@base.route("/ping")
def ping():
    """Ping the app."""
    return {"success": True}


@base.route("/version")
def version():
    """Get the app version."""
    return {"version": VERSION_FILE.read_text().strip()}
