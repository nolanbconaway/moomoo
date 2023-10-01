"""Score all local files and insert the embeddings into the database."""
import sys
from pathlib import Path

import click
from tqdm import tqdm

from .. import utils_
from ..db import FileEmbedding, execute_sql_fetchall, get_session
from .scorer import Model

EXTENSIONS = set([".mp3", ".flac"])


def list_audio_files(src_dir: Path) -> list[Path]:
    """List all audio files in the directories."""
    return [
        p
        for p in src_dir.rglob("**/*")
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]


@click.command()
@click.argument(
    "src_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("artifacts"),
    show_default=True,
    help="Path to the saved artifacts directory.",
)
def main(src_dir: Path, artifacts: Path):
    """Score local files and insert embeddings into the db.

    Checks the database for files that have already been scored and skips them.
    """
    if not FileEmbedding.exists():
        click.echo(
            f"Table {FileEmbedding.table_name()} does not exist. "
            + f"Use `moomoo db create {FileEmbedding.table_name()}` to create it."
        )
        sys.exit(1)

    click.echo("Listing unscored media files.")
    sql = f"select filepath from {FileEmbedding().full_name()}"
    already_scored = set(
        [src_dir / row["filepath"] for row in execute_sql_fetchall(sql)]
    )
    all_files = set(list_audio_files(src_dir))
    unscored_files = all_files - already_scored

    click.echo(f"Found {len(unscored_files)} unscored file(s).")
    if len(unscored_files) == 0:
        click.echo("Nothing to do.")
        sys.exit(0)

    click.echo("Loading model.")
    model = Model.from_artifacts(artifacts)
    click.echo(f"Model loaded at device: {model.device}.")

    click.echo("Scoring files.")
    with get_session() as session:
        for filepath in tqdm(unscored_files, disable=None):
            embedding = model.score(filepath)
            relative_path = filepath.relative_to(src_dir)
            if not embedding.success:
                click.echo(f"Failed to score {relative_path}: {embedding.fail_reason}.")

            FileEmbedding(
                filepath=str(relative_path),
                insert_ts_utc=utils_.utcnow(),
                **embedding.to_dict(),
            ).upsert(session=session)

    click.echo("Done.")
