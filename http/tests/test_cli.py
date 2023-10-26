"""Test that the version command works."""
from moomoo_http.cli import cli
from click.testing import CliRunner


def test_cli__version():
    runner = CliRunner()
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert "." in result.output


def test_cli__inidtb():
    runner = CliRunner()
    result = runner.invoke(cli, ["initdb"])
    assert result.exit_code == 0