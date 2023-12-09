"""Connectivity utils for the database."""


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


if __name__ == "__main__":
    cli()
