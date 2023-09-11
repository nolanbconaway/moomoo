"""Create the Flask app."""
import argparse
import logging

import click
import waitress
from flask import Flask


def create_app() -> Flask:
    """Create a Flask app."""
    app = Flask("moomoo")
    app.add_url_rule("/ping", view_func=lambda: {"success": True})

    from . import playlist

    app.register_blueprint(playlist.bp)

    return app


def run_wsgi(host: str, port: int) -> None:
    """Run the WSGI server."""
    app = create_app()
    logger = logging.getLogger("waitress")
    logger.setLevel(logging.INFO)

    click.echo(f"Starting moomoo http server on {host}:{port}")
    waitress.serve(app, host=host, port=port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    run_wsgi(args.host, args.port)
