"""Test that the version command works."""
from click.testing import CliRunner
from moomoo_ingest.cli import cli


def test_cli_version():
    runner = CliRunner()

    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert "." in result.output
