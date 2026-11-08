import click
from moomoo_pg import get_session
from sqlalchemy import text

from . import sync
from .ddl import MusicBrainzBase


@click.group()
def cli():
    pass


@cli.command("create-tables")
def create_tables() -> None:
    with get_session() as session:
        session.execute(text("CREATE SCHEMA IF NOT EXISTS musicbrainz"))
        session.commit()
        MusicBrainzBase.metadata.create_all(session.get_bind())


@cli.command("copy")
@click.option("--batch-size", default=1000, show_default=True)
@click.option("--table", "tables", multiple=True, type=click.Choice(sync.TABLE_NAMES))
def run_copy(batch_size: int, tables: tuple[str, ...]) -> None:
    sync.run_copy(batch_size, tables)
