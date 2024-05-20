"""Connectivity utils for the database."""

import re
from pathlib import Path

import click

from . import ddl

TABLE_NAMES: tuple[str] = tuple([table.__tablename__ for table in ddl.TABLES])


@click.group
def cli() -> None:
    """Database handlers."""


@cli.command("ddl")
@click.argument("table_name", type=click.Choice(TABLE_NAMES), required=False)
@click.option("--all", "all_", is_flag=True, default=False)
def cli_print_ddl(table_name: str, all_: bool) -> None:
    """Print database DDL."""
    if not (table_name or all_):
        raise click.UsageError("Must specify either --all or a table name.")

    if all_ and table_name:
        raise click.UsageError("Must specify either --all or a table name, not both.")

    if all_:
        for table in ddl.TABLES:
            for stmt in table.ddl():
                click.echo(str(stmt) + ";")
    else:
        table = next(table for table in ddl.TABLES if table.__tablename__ == table_name)
        for stmt in table.ddl():
            click.echo(str(stmt) + ";")


@cli.command("create")
@click.argument("table_name", type=click.Choice(TABLE_NAMES), required=True)
@click.option("--drop", is_flag=True, default=False)
@click.option("--if-not-exists", is_flag=True, default=False)
def cli_create_tables(table_name: str, drop: bool, if_not_exists: bool) -> None:
    """Create database tables."""
    table = next(table for table in ddl.TABLES if table.__tablename__ == table_name)

    if drop:
        click.echo(f"Dropping table {table_name}...")
        table.drop(if_exists=True)

    click.echo(f"Creating table {table_name}...")
    table.create(if_not_exists=if_not_exists)


@cli.command("add-exclude-path")
@click.argument("path", type=click.Path(path_type=Path, exists=True, resolve_path=True))
@click.option(
    "--library",
    type=click.Path(exists=True, file_okay=False, path_type=Path, resolve_path=True),
    envvar="MOOMOO_MEDIA_LIBRARY",
    required=True,
)
@click.option("--note", type=str, required=False, default=None)
def cli_add_exclude_path(path: Path, library: Path, note: str | None) -> None:
    """Add a path to the exclude list."""

    if path == library:
        raise click.UsageError("Cannot exclude the media library path.")

    # make path relative to media library and escape it
    pattern = re.escape(str(path.relative_to(library)))

    # append to the string to ensure start of path is matched
    pattern = f"^{pattern}"

    click.echo(f"Adding {path} to the exclude list.")

    ddl.LocalFileExcludeRegex(pattern=pattern, note=note).insert()

    click.echo("Successfully added path to the exclude list.")


if __name__ == "__main__":
    cli()
