"""Cli group for moomoo."""
import click

from .annotate_mbids import main as annotate_mbids_main
from .artist_stats import main as artist_stats_main
from .collect_listen_data import main as collect_listen_data_main
from .collect_local_files import main as collect_local_files_main
from .collect_similar_user_activity import main as collect_similar_user_activity_main
from .db.cli import cli as db_cli
from .utils_ import moomoo_version


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


@cli.command("version")
def version_cli():
    """Print the version."""
    click.echo(moomoo_version())

cli.add_command(db_cli, "db")
cli.add_command(annotate_mbids_main, "annotate-mbids")
cli.add_command(artist_stats_main, "artist-stats")
cli.add_command(collect_listen_data_main, "listens")
cli.add_command(collect_local_files_main, "local-files")
cli.add_command(collect_similar_user_activity_main, "similar-user-activity")


if __name__ == "__main__":
    cli()
