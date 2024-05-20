"""Cli handlers for moomoo machine learning."""

import datetime
import json
import re
import sys
from pathlib import Path

import click
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
from transformers import AutoModel, Wav2Vec2FeatureExtractor

from .db import BaseTable, FileEmbedding, LocalFileExcludeRegex, get_session
from .scorer import Model

MODEL_INFO = json.loads((Path(__file__).parent / "model-info.json").read_text())
EXTENSIONS = set([".mp3", ".flac"])
VERSION = (Path(__file__).parent / "version").read_text().strip()


def list_audio_files(src_dir: Path) -> list[Path]:
    """List all audio files in the directories."""
    return [
        p
        for p in src_dir.rglob("**/*")
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]


def pass_all_exclude_rules(
    path: Path, src_dir: Path, regexes: list[re.Pattern[str]]
) -> bool:
    """Return True if the path passes all the exclude regexes.

    Split out in this way to support multiprocessing, testing.
    """
    return not any(regex.match(str(path.relative_to(src_dir))) for regex in regexes)


@click.group()
def cli():
    """Cli group for moomoo ml."""
    pass


@cli.command("version")
def version():
    """Print the version."""
    click.echo(VERSION)


@cli.command("create-db")
def create_db():
    """Create the database."""
    with get_session() as session:
        engine = session.get_bind()
        BaseTable.metadata.create_all(engine, tables=[FileEmbedding.__table__])


@cli.command("save-artifacts")
@click.argument("output", type=click.Path(path_type=Path), default=Path("artifacts"))
@click.option("--model", "model_name", default=MODEL_INFO["name"], show_default=True)
@click.option("--revision", default=MODEL_INFO["revision"], show_default=True)
def save_artifacts(output: Path, model_name: str, revision: str):
    """Save artifacts for the ml model."""
    if not output.exists():
        output.mkdir(parents=True)
    else:
        for p in output.iterdir():
            p.unlink()

    click.echo(f"Saving Wav2Vec2FeatureExtractor artifact to {output}.")
    Wav2Vec2FeatureExtractor.from_pretrained(
        model_name, trust_remote_code=True, revision=revision
    ).save_pretrained(output)

    click.echo(f"Saving AutoModel artifact to {output}.")
    AutoModel.from_pretrained(
        model_name, trust_remote_code=True, revision=revision
    ).save_pretrained(output)


@cli.command("score")
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
def score_local_files(src_dir: Path, artifacts: Path):
    """Score local files and insert embeddings into the db.

    Checks the database for files that have already been scored and skips them.
    """
    click.echo("Listing unscored media files.")
    with get_session() as session:
        already_scored = set(
            [src_dir / i for (i,) in session.query(FileEmbedding.filepath)]
        )
    all_files = set(list_audio_files(src_dir))
    unscored_files = all_files - already_scored

    click.echo(f"Found {len(unscored_files)} unscored file(s).")

    # filter out files that match the exclude regexes
    exclude_regexes = LocalFileExcludeRegex.fetch_all_regex()
    unscored_files = set(
        [
            p
            for p in unscored_files
            if pass_all_exclude_rules(p, src_dir, exclude_regexes)
        ]
    )
    click.echo(f"Found {len(unscored_files)} file(s) after filtering by regex.")

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

            # upsert the embedding
            stmt = (
                insert(FileEmbedding)
                .values(filepath=str(relative_path), **embedding.to_dict())
                .on_conflict_do_update(
                    index_elements=[FileEmbedding.filepath],
                    set_=dict(
                        success=embedding.success,
                        fail_reason=embedding.fail_reason,
                        duration_seconds=embedding.duration_seconds,
                        embedding=embedding.embedding,
                        insert_ts_utc=datetime.datetime.utcnow().replace(
                            tzinfo=datetime.timezone.utc
                        ),
                    ),
                )
            )
            session.execute(stmt)
            session.commit()

    click.echo("Done.")


if __name__ == "__main__":
    cli()
