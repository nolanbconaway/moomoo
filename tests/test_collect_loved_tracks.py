import datetime
import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from lastfmrec import collect_loved_tracks, utils_

from .conftest import MockResponse

RESOURCES = Path(__file__).parent / "resources"


def load_resource_json(name):
    return json.loads((RESOURCES / name).read_text())


@pytest.fixture(autouse=True)
def mock_insert(monkeypatch):
    monkeypatch.setattr(collect_loved_tracks, "insert", lambda *args, **kwargs: ...)


def test_get_loves_by_page(monkeypatch):
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("lovedtracks_api_data.json")),
    )
    res = collect_loved_tracks.get_loves_by_page(username="FAKE")
    assert len(res) == 2


def test_cli_main(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: MockResponse(load_resource_json("lovedtracks_api_data.json")),
    )

    result = runner.invoke(
        collect_loved_tracks.main, ["FAKE_NAME", "--table=FAKE", "--schema=FAKE"]
    )
    assert result.exit_code == 0
    assert "Inserting" in result.output
