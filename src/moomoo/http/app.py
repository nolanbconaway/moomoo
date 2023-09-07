"""Create the Flask app."""
import argparse
import logging

import waitress
from flask import Flask


def create_app() -> Flask:
    """Create a Flask app."""
    app = Flask("moomoo")

    from . import playlist

    app.register_blueprint(playlist.bp)

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    app = create_app()
    logger = logging.getLogger("waitress")
    logger.setLevel(logging.INFO)
    waitress.serve(app, host=args.host, port=args.port)
