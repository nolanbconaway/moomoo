"""Cli handlers for moomoo machine learning."""
import json
from pathlib import Path

import click
from transformers import AutoModel, Wav2Vec2FeatureExtractor

from . import score_local_files

MODEL_INFO = json.loads((Path(__file__).parent / "model-info.json").read_text())


@click.group()
def cli():
    """Cli group for moomoo ml."""
    pass


cli.add_command(score_local_files.main, "score")


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


if __name__ == "__main__":
    cli()
