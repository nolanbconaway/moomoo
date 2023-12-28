import json
import socket
from pathlib import Path

import pytest
from click.testing import CliRunner
from requests_mock import Mocker as RequestsMocker

from moomoo_client.cli import cli as client_cli
from moomoo_client.cli import playlist as playlist_cli


@pytest.fixture(autouse=True)
def moomoo_host(monkeypatch) -> str:
    """Mock the moomoo host to be localhost on a random unused port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        _, port = s.getsockname()

    url = f"mock://localhost:{port}"
    monkeypatch.setenv("MOOMOO_HOST", url)
    yield url


@pytest.fixture(autouse=True)
def local_files(monkeypatch, tmp_path) -> Path:
    """Override the local files path and set it to a temporary directory."""
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(tmp_path))
    tmp_path = Path(tmp_path)
    (tmp_path / "test.mp3").touch()
    yield tmp_path


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(client_cli, ["version"])
    assert result.exit_code == 0
    assert "." in result.output


def test_playlist_from_path__media_library_exists_check(
    monkeypatch, moomoo_host: str, local_files: Path, requests_mock: RequestsMocker
):
    """Test that the media library exists check works."""
    # mock out the request to the server
    requests_mock.get(
        f"{moomoo_host}/playlist/from-files",
        json={"success": True, "playlists": [{"playlist": [], "description": None}]},
    )

    runner = CliRunner()
    result = runner.invoke(playlist_cli, ["from-path", str(local_files / "test.mp3")])
    assert result.exit_code == 0

    # set the media library to a non-existent path, should fail
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(local_files) + "fakeeee")
    result = runner.invoke(playlist_cli, ["from-path", str(local_files / "test.mp3")])
    assert result.exit_code != 0
    assert str(local_files) + "fakeeee" + " does not exist" in str(result.exception)


def test_playlist_from_path__json_output(
    moomoo_host: str, local_files: Path, requests_mock: RequestsMocker
):
    """Test that the media library exists check works."""
    # mock out the request to the server
    requests_mock.get(
        f"{moomoo_host}/playlist/from-files",
        json={
            "success": True,
            "playlists": [{"playlist": ["a", "b", "c"], "description": "aaa"}],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        playlist_cli,
        ["from-path", str(local_files / "test.mp3"), "--out=json"],
    )
    assert result.exit_code == 0

    # json loadable
    data = json.loads(result.output)
    assert data["playlist"] == [
        str(local_files / "a"),
        str(local_files / "b"),
        str(local_files / "c"),
    ]


def test_playlist_from_path__error_handling(
    local_files: Path, requests_mock: RequestsMocker, moomoo_host: str
):
    """Test that the error handling works."""
    # return a 500 error from the server
    requests_mock.get(
        f"{moomoo_host}/playlist/from-files",
        json={"success": False, "error": "NoFilesRequestedError"},
        status_code=500,
    )

    runner = CliRunner()

    result = runner.invoke(
        playlist_cli, ["from-path", str(local_files / "test.mp3")]
    )
    assert result.exit_code != 0

    # should still be json parsable
    assert len(result.output) > 0
    assert json.loads(result.output)
    assert json.loads(result.output)["success"] is False
    assert "NoFilesRequestedError" in json.loads(result.output)["error"]
