"""Create the Flask app."""

import os
from pathlib import Path

from flask import Flask

VERSION_FILE = Path(__file__).parent / "version"


def create_app() -> Flask:
    """Create a Flask app."""
    app = Flask("moomoo")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["MOOMOO_POSTGRES_URI"]

    from .db import db
    from .routes import app_meta, playlist

    app.register_blueprint(playlist.base)
    app.register_blueprint(app_meta.base)
    db.init_app(app)

    return app
