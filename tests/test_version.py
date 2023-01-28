"""Test that the version command works."""
from moomoo.cli import cli
from click.testing import CliRunner


def test_cli_version():
    runner = CliRunner()

    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert "." in result.output
