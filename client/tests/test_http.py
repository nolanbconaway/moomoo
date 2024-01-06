"""Test the utils module."""

import asyncio
import socket
from pathlib import Path

import pytest
from httpx import HTTPStatusError
from moomoo_client.http import Playlist, PlaylistRequester
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


@pytest.fixture
def local_files(monkeypatch, tmp_path) -> Path:
    """Override the local files path and set it to a temporary directory."""
    monkeypatch.setenv("MOOMOO_MEDIA_LIBRARY", str(tmp_path))
    tmp_path = Path(tmp_path)
    (tmp_path / "test.mp3").touch()
    yield tmp_path


def test_PlaylistRequester__host(monkeypatch):
    """Test the host property."""
    monkeypatch.setenv("MOOMOO_HOST", "localhost:8000")
    assert PlaylistRequester().host == "localhost:8000"

    monkeypatch.delenv("MOOMOO_HOST", raising=False)
    with pytest.raises(ValueError):
        # there is an exists check in the property
        PlaylistRequester().host  # noqa: B018


def test_PlaylistRequester__request_tuples(monkeypatch):
    """Test the request_tuples method."""
    monkeypatch.setenv("MOOMOO_HOST", "localhost:8000")

    requester = PlaylistRequester()
    assert requester.request_tuples() == [("n", 20), ("seed", 1), ("shuffle", True)]

    requester = PlaylistRequester(tracks=10, seed=2, shuffle=False)
    assert requester.request_tuples() == [("n", 10), ("seed", 2), ("shuffle", False)]


@pytest.mark.asyncio
async def test_PlaylistRequester__make_request__error_handling(httpx_mock: HTTPXMock):
    """Test the make_request method's error handling."""
    httpx_mock.add_response(
        json={"success": False, "error": "fake error message"}, status_code=500
    )

    # handle explicit http errors
    requester = PlaylistRequester()
    with pytest.raises(HTTPStatusError):
        await requester.make_request("/endpoint")

    # handle 200s but unsuccessful requests
    httpx_mock.add_response(
        json={"success": False, "error": "fake error message"}, status_code=200
    )
    with pytest.raises(RuntimeError):
        await requester.make_request("/endpoint")


@pytest.mark.asyncio
async def test_PlaylistRequester__request_playlist_from_path(
    local_files: Path, httpx_mock: HTTPXMock
):
    """Test the request_playlist_from_path method."""
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [{"playlist": ["a", "b", "c"], "description": "aaa"}],
        },
    )

    # all good
    requester = PlaylistRequester()
    res = await requester.request_playlist_from_path([local_files / "test.mp3"])
    assert isinstance(res, Playlist)
    assert res.playlist == [local_files / "a", local_files / "b", local_files / "c"]
    assert res.description == "aaa"


@pytest.mark.asyncio
async def test_PlaylistRequester__request_user_artist_suggestions(
    local_files: Path, httpx_mock: HTTPXMock
):
    """Test the request_playlist_from_path method."""
    httpx_mock.add_response(
        json={
            "success": True,
            "playlists": [{"playlist": ["a", "b", "c"], "description": "aaa"}],
        },
    )

    requester = PlaylistRequester()
    res = await requester.request_user_artist_suggestions("username", 3)
    assert len(res) == 1

    plist = res[0]
    assert isinstance(plist, Playlist)
    assert plist.playlist == [local_files / "a", local_files / "b", local_files / "c"]
    assert plist.description == "aaa"
