"""Cli handlers for moomoo machine learning."""

import datetime
import json
import re
import sys
from pathlib import Path

import click
import numpy as np
from sqlalchemy import text, update
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
from transformers import AutoModel, Wav2Vec2FeatureExtractor

from .conditioner import Model as ConditionerModel
from .db import BaseTable, FileEmbedding, LocalFileExcludeRegex, get_session
from .scorer import Model

MODEL_INFO = json.loads((Path(__file__).parent / "model-info.json").read_text())
EXTENSIONS = set([".mp3", ".flac"])
VERSION = (Path(__file__).parent / "version").read_text().strip()


def list_audio_files(src_dir: Path) -> list[Path]:
    """List all audio files in the directories."""
    return [p for p in src_dir.rglob("**/*") if p.is_file() and p.suffix.lower() in EXTENSIONS]


def pass_all_exclude_rules(path: Path, src_dir: Path, regexes: list[re.Pattern[str]]) -> bool:
    """Return True if the path passes all the exclude regexes.

    Split out in this way to support multiprocessing, testing.
    """
    return not any(regex.match(str(path.relative_to(src_dir))) for regex in regexes)


def get_db_embeddings(unconditioned: bool = False) -> tuple[list[Path], np.ndarray]:
    """Get embeddings from the database.

    Returns a list of Paths and a 2d numpy array of embeddings. Each row in the array corresponds to
    the embedding of the file at the same index in the list.

    If unconditioned is provided, only embeddings which have not been conditioned will be returned.
    """
    if unconditioned:
        click.echo("Getting unconditioned embeddings.")
        conditioner_sql = FileEmbedding.conditioned_embedding.is_(None)
    else:
        click.echo("Getting all embeddings.")
        conditioner_sql = text("true")

    with get_session() as session:
        query = (
            session.query(FileEmbedding)
            .filter(FileEmbedding.success.is_(True))
            .filter(conditioner_sql)
            .order_by(FileEmbedding.filepath)
        )

        if not query.count():
            paths, embeddings = [], []
        else:
            paths, embeddings = zip(*[(Path(i.filepath), i.embedding) for i in query.all()])

    click.echo(f"Found {len(paths)} embedding(s).")
    return paths, np.array(embeddings)


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
        BaseTable.metadata.create_all(
            engine, tables=[FileEmbedding.__table__, LocalFileExcludeRegex.__table__]
        )


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
@click.argument("src_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
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
        already_scored = set([src_dir / i for (i,) in session.query(FileEmbedding.filepath)])
    all_files = set(list_audio_files(src_dir))
    unscored_files = all_files - already_scored

    click.echo(f"Found {len(unscored_files)} unscored file(s).")

    # filter out files that match the exclude regexes
    exclude_regexes = LocalFileExcludeRegex.fetch_all_regex()
    unscored_files = set(
        [p for p in unscored_files if pass_all_exclude_rules(p, src_dir, exclude_regexes)]
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
                        insert_ts_utc=datetime.datetime.now(datetime.timezone.utc),
                    ),
                )
            )
            session.execute(stmt)
            session.commit()

    click.echo("Done.")


@cli.command("build-conditioner")
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("artifacts"),
    show_default=True,
    help="Path to the saved artifacts directory.",
)
@click.option("--rerun", "rerun", is_flag=True, help="Option to rerun the conditioning.")
def build_conditioner(artifacts: Path, rerun: bool):
    """Build a conditioner model and save artifacts."""
    click.echo("Building conditioner model.")
    _, embeds = get_db_embeddings()
    model = ConditionerModel()
    model.fit(embeds)

    click.echo("")
    click.secho("Conditioner model built.", fg="green")
    click.secho(f"  Name: {model.name}", fg="green")
    click.secho(f"  Hash: {model.hash}", fg="green")
    click.secho(f"  Filename: {model.filename}", fg="green")
    click.echo("")

    model.save_to_artifacts(artifacts)
    click.echo(f"Conditioner model saved to {artifacts}/{model.filename}.")

    # ask user if they want to update model-info.json
    if click.confirm("Update conditioner-info.json?", abort=True):
        model.update_model_info()

    # run condition-new-files
    if rerun and click.confirm("Update conditioned embeddings in the database?", abort=True):
        condition_new_files(["--artifacts", str(artifacts), "--rerun"])


@cli.command("condition")
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path("artifacts"),
    show_default=True,
    help="Path to the saved artifacts directory.",
)
@click.option("--rerun", "rerun", is_flag=True, help="Option to rerun the conditioning.")
def condition_new_files(artifacts: Path, rerun: bool):
    """Write the conditioned embeddings to the database.

    Skips files that have already been conditioned for the given conditioner.
    """
    if rerun and click.confirm(
        "This command will delete all conditioned embeddings. Continue?", abort=True
    ):
        click.echo("Dropping all conditioned embeddings.")
        with get_session() as session:
            stmt = update(FileEmbedding).values(conditioned_embedding=None)
            session.execute(stmt)
            session.commit()

    click.echo("Loading conditioner model.")
    model = ConditionerModel.load_from_artifacts(artifacts)
    paths, raw_embeds = get_db_embeddings(unconditioned=True)

    if not paths:
        click.echo("No new files to condition.")
        sys.exit(0)

    click.echo("Scoring new files.")
    conditioned_embeds = model.transform(raw_embeds)

    click.echo(f"Saving {len(paths)} conditioned embeddings.")
    with get_session() as session:
        for idx, path in tqdm(enumerate(paths), total=len(paths), disable=None):
            stmt = (
                update(FileEmbedding)
                .where(FileEmbedding.filepath == str(path))
                .values(conditioned_embedding=conditioned_embeds[idx, :].tolist())
            )
            session.execute(stmt)
            session.commit()


if __name__ == "__main__":
    cli()
