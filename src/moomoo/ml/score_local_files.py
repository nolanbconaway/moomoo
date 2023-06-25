"""Score all local files and insert the embeddings into the database."""
import sys
from pathlib import Path

import click
from psycopg import Connection
from tqdm import tqdm

from .. import utils_
from .scorer import Model
from .storage import DDL, insert_embedding, list_audio_files


@click.command()
@click.argument(
    "src_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option("--table", required=True)
@click.option("--schema", required=True)
@click.option(
    "--create", is_flag=True, help="Option to teardown and recreate the table"
)
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("artifacts"),
    show_default=True,
    help="Path to the saved artifacts directory.",
)
def main(src_dir: Path, table: str, schema: str, create: bool, artifacts: Path):
    """Score local files and insert embeddings into the db.

    Checks the database for files that have already been scored and skips them.
    """
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    click.echo("Listing unscored media files.")
    sql = f"select filepath from {schema}.{table}"
    already_scored = set([src_dir / i for (i,) in utils_.execute_sql_fetchall(sql)])
    all_files = set(list_audio_files(src_dir))
    unscored_files = all_files - already_scored

    click.echo(f"Found {len(unscored_files)} unscored files.")
    if len(unscored_files) == 0:
        click.echo("Nothing to do.")
        sys.exit(0)

    click.echo("Loading model.")
    model = Model.from_artifacts(artifacts)
    click.echo(f"Model loaded at device: {model.device}.")

    click.echo("Scoring files.")
    with utils_.pg_connect() as conn:
        for filepath in tqdm(unscored_files, disable=None):
            embedding = model.score(filepath)
            relative_path = filepath.relative_to(src_dir)
            if not embedding.success:
                click.echo(f"Failed to score {relative_path}: {embedding.fail_reason}.")

            insert_embedding(
                conn=conn,
                filepath=relative_path,
                embedding=embedding,
                schema=schema,
                table=table,
            )
