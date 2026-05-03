import click

from moomoo_navidrome.jobs.playlist import cli as playlist_cli


@click.group()
def cli():
    """Command line interface for moomoo navidrome integrations."""
    pass


cli.add_command(playlist_cli, "playlist")


if __name__ == "__main__":
    cli()
