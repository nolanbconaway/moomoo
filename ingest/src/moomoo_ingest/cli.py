"""Cli group for moomoo."""

import click

from .artist_stats import main as artist_stats_main
from .collect_listen_data import main as collect_listen_data_main
from .collect_listenbrainz_data_dump import main as collect_listenbrainz_data_dump_main
from .collect_listenbrainz_feedback import main as collect_listenbrainz_feedback_main
from .collect_local_files import main as collect_local_files_main
from .collect_msid_map import main as collect_msid_map_main
from .collect_musicbrainz_data_dump import main as collect_musicbrainz_data_dump_main
from .collect_similar_user_activity import main as collect_similar_user_activity_main
from .musicbrainz.cli import cli as musicbrainz_cli
from .update_artist_similarity_matrix import main as update_artist_similarity_matrix_main
from .utils_ import moomoo_version


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


@cli.command("version")
def version_cli():
    """Print the version."""
    click.echo(moomoo_version())


cli.add_command(artist_stats_main, "artist-stats")
cli.add_command(collect_listen_data_main, "listens")
cli.add_command(collect_local_files_main, "local-files")
cli.add_command(collect_similar_user_activity_main, "similar-user-activity")
cli.add_command(collect_msid_map_main, "msid-map")
cli.add_command(collect_listenbrainz_feedback_main, "listenbrainz-feedback")
cli.add_command(collect_listenbrainz_data_dump_main, "listenbrainz-data-dump")
cli.add_command(update_artist_similarity_matrix_main, "update-artist-similarity-matrix")
cli.add_command(collect_musicbrainz_data_dump_main, "musicbrainz-data-dump")
cli.add_command(musicbrainz_cli, "musicbrainz")


if __name__ == "__main__":
    cli()
