from click.testing import CliRunner
from moomoo_ml.cli import version


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(version)
    assert result.exit_code == 0
