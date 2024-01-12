import json
import socket
from pathlib import Path

import pytest
from click.testing import CliRunner
from moomoo_client.cli.cli import cli as client_cli
from moomoo_client.cli.playlist import cli as playlist_cli
from pytest_httpx import HTTPXMock


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


def test_playlist_from_path(local_files: Path, httpx_mock: HTTPXMock):
    """End to end test for playlist from path."""
    # mock out the request to the server
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [
                {
                    "playlist": [{"filepath": "a"}, {"filepath": "b"}],
                    "description": "aaa",
                }
            ],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        playlist_cli,
        ["from-path", str(local_files / "test.mp3"), "--out=json"],
    )
    assert result.exit_code == 0

    # json loadable
    data = json.loads(result.output.splitlines()[-1])
    assert data["playlist"] == [str(local_files / "a"), str(local_files / "b")]


def test_playlist_loved(local_files: Path, httpx_mock: HTTPXMock):
    """End to end test for playlist from loved tracks."""
    # mock out the request to the server
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [
                {
                    "playlist": [{"filepath": "a"}, {"filepath": "b"}],
                    "description": "aaa",
                }
            ],
        },
    )

    runner = CliRunner()
    result = runner.invoke(playlist_cli, ["loved", "username", "--out=json"])
    assert result.exit_code == 0

    # json loadable
    data = json.loads(result.output.splitlines()[-1])
    assert data["playlist"] == [str(local_files / "a"), str(local_files / "b")]


def test_playlist_suggested_artists(local_files: Path, httpx_mock: HTTPXMock):
    """End to end test for suggested-artists."""
    # mock out the request to the server
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [
                {
                    "playlist": [{"filepath": "a"}, {"filepath": "b"}],
                    "description": "aaa",
                }
            ],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        playlist_cli, ["suggest-artists", "username", "--out=json"], input="0"
    )
    assert result.exit_code == 0

    # json loadable
    data = json.loads(result.output.splitlines()[-1])
    assert data["playlist"] == [str(local_files / "a"), str(local_files / "b")]


def test_playlist_revisit_releases(local_files: Path, httpx_mock: HTTPXMock):
    """End to end test for revisit-releases."""
    # mock out the request to the server
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [
                {
                    "playlist": [{"filepath": "a"}, {"filepath": "b"}],
                    "description": "aaa",
                }
            ],
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        playlist_cli, ["revisit-releases", "username", "--out=json"], input="0"
    )
    assert result.exit_code == 0

    # json loadable
    data = json.loads(result.output.splitlines()[-1])
    assert data["playlist"] == [str(local_files / "a"), str(local_files / "b")]
