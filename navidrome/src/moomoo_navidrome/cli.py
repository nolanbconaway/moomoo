import click

from moomoo_navidrome.jobs.loves import cli as loves_cli
from moomoo_navidrome.jobs.play_queue import cli as play_queue_cli
from moomoo_navidrome.jobs.playlist import cli as playlist_cli


@click.group()
def cli():
    """Command line interface for moomoo navidrome integrations."""
    pass


cli.add_command(playlist_cli, "playlist")
cli.add_command(loves_cli, "loves")
cli.add_command(play_queue_cli, "play-queue")


if __name__ == "__main__":
    cli()
