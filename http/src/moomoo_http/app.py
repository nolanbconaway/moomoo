"""Create the Flask app."""
import os

from flask import Flask


def create_app() -> Flask:
    """Create a Flask app."""
    app = Flask("moomoo")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["MOOMOO_POSTGRES_URI"]

    app.add_url_rule("/ping", view_func=lambda: {"success": True})

    from .db import db
    from .routes import playlist

    app.register_blueprint(playlist.base)
    db.init_app(app)

    return app
