"""Cli handlers for moomoo machine learning."""

import sys
from pathlib import Path

import click
import numpy as np
from sqlalchemy import update
from tqdm import tqdm

from ..db import FileEmbedding, get_session
from .conditioner import Model as ConditionerModel

DEFAULT_ARTIFACTS_PATH = Path("artifacts")


def transform_and_save_to_db(model: ConditionerModel, paths: list[Path], embeds: np.ndarray):
    """Transform embeddings and save to the database."""
    if not model.is_fitted:
        raise ValueError("Model not fitted.")

    if len(paths) != embeds.shape[0]:
        raise ValueError("Number of paths and embeddings do not match.")

    if not paths:
        click.echo("No new files to condition.")
        return

    click.echo(f"Conditioning {len(paths)} embeddings.")
    conditioned = model.transform(embeds)

    click.echo(f"Saving {len(paths)} conditioned embeddings.")
    with get_session() as session:
        for idx, path in tqdm(enumerate(paths), total=len(paths), disable=None):
            stmt = (
                update(FileEmbedding)
                .where(FileEmbedding.filepath == str(path))
                .values(conditioned_embedding=conditioned[idx, :].tolist())
            )
            session.execute(stmt)
            session.commit()


def drop_existing_conditioned_embeddings():
    """Drop all existing conditioned embeddings."""
    click.echo("Dropping all conditioned embeddings.")
    with get_session() as session:
        stmt = update(FileEmbedding).values(conditioned_embedding=None)
        session.execute(stmt)
        session.commit()


@click.group("conditioner")
def cli():
    """Cli group for moomoo ml."""
    pass


@cli.command("build")
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    # use lambda to avoid loading the default path before the option is parsed
    default=lambda: DEFAULT_ARTIFACTS_PATH,
    show_default=True,
    help="Path to the saved artifacts directory.",
)
@click.option(
    "--update-info", "update_info", is_flag=True, help="Option to update model-info.json."
)
@click.option(
    "--replace-embeds", "replace_embeds", is_flag=True, help="Option to replace embeddings."
)
def build_conditioner(artifacts: Path, update_info: bool, replace_embeds: bool):
    """Build a conditioner model and save artifacts."""
    if replace_embeds and not update_info:
        raise click.UsageError(
            "Cannot use --replace-embeds without --update-info. "
            + "This will result in conditioned embeddings without a saved model."
        )

    click.echo("Building conditioner model.")
    paths, embeds = FileEmbedding.fetch_numpy_embeddings()
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

    if update_info:
        model.update_model_info()
        click.echo(f"Model info updated in {ConditionerModel.INFO_FILE}.")

    if replace_embeds and click.confirm(
        "--replace-embeds will delete and replace all conditioned embeddings. Continue?", abort=True
    ):
        drop_existing_conditioned_embeddings()
        transform_and_save_to_db(model, paths, embeds)


@cli.command("score")
@click.option(
    "--artifacts",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    # use lambda to avoid loading the default path before the option is parsed
    default=lambda: DEFAULT_ARTIFACTS_PATH,
    show_default=True,
    help="Path to the saved artifacts directory.",
)
@click.option("--replace", "replace", is_flag=True, help="Option to replace existing embeddings.")
def condition_new_files(artifacts: Path, replace: bool):
    """Write the conditioned embeddings to the database.

    Skips files that have already been conditioned for the given conditioner.
    """
    if replace and click.confirm(
        "This command will delete all conditioned embeddings. Continue?", abort=True
    ):
        drop_existing_conditioned_embeddings()

    click.echo("Loading conditioner model.")
    model = ConditionerModel.load_from_artifacts(artifacts)
    paths, raw_embeds = FileEmbedding.fetch_numpy_embeddings(only_unconditioned=not replace)

    if not paths:
        click.echo("No new files to condition.")
        sys.exit(0)

    transform_and_save_to_db(model, paths, raw_embeds)


if __name__ == "__main__":
    cli()
