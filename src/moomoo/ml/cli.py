"""Cli handlers for moomoo machine learning."""
from pathlib import Path

import click
from transformers import AutoModel, Wav2Vec2FeatureExtractor

from . import score_local_files


@click.group()
def cli():
    """Cli group for moomoo ml."""
    pass


cli.add_command(score_local_files.main, "score")


@cli.command("save-artifacts")
@click.argument("output", type=click.Path(path_type=Path), default=Path("artifacts"))
@click.option("--model", "model_name", default="m-a-p/MERT-v1-330M")
@click.option("--revision", default="af10da7")
def save_artifacts(output: Path, model_name: str, revision: str):
    """Save artifacts for the ml model.

    Default uses https://huggingface.co/m-a-p/MERT-v1-330M (af10da7) and saves to
    a local artifacts folder.
    """
    if not output.exists():
        output.mkdir(parents=True)
    else:
        for p in output.iterdir():
            p.unlink()

    Wav2Vec2FeatureExtractor.from_pretrained(
        model_name, trust_remote_code=True, revision=revision
    ).save_pretrained(output)

    AutoModel.from_pretrained(
        model_name, trust_remote_code=True, revision=revision
    ).save_pretrained(output)


if __name__ == "__main__":
    cli()
