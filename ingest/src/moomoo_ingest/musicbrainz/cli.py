import click

from . import sync


@click.group()
def cli():
    pass


@cli.command("copy")
@click.option("--batch-size", default=1000, show_default=True)
@click.option("--table", "tables", multiple=True, type=click.Choice(sync.TABLE_NAMES))
def run_copy(batch_size: int, tables: tuple[str, ...]) -> None:
    sync.run_copy(batch_size, tables)
