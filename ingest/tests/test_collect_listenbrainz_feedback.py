from unittest import mock
from uuid import uuid1

from click.testing import CliRunner

from moomoo_ingest import collect_listenbrainz_feedback, utils_
from moomoo_ingest.db import ListenBrainzUserFeedback


def get_mock_lb_http(*responses) -> mock.Mock:
    """Make patched objects for the whole module."""

    def side_effect(*args, **kwargs):
        yield from responses

    return mock.patch(
        "moomoo_ingest.collect_listenbrainz_feedback.ListenBrainz._get",
        side_effect=side_effect(),
    )


def test_cli_main__no_data(monkeypatch):
    """Test the main function with empty data in the db and api."""
    ListenBrainzUserFeedback.create()

    # no db loves, no api loves. do nothing
    monkeypatch.setattr(collect_listenbrainz_feedback, "get_total_feedback_count", lambda _: 0)
    with get_mock_lb_http(dict(feedback=[])):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])

    assert result.exit_code == 0
    assert "No loves found via api." in result.output
    assert len(ListenBrainzUserFeedback.select_star()) == 0


def test_cli_main__some_data(monkeypatch):
    """Test the main function with empty data in the db but some in the api."""
    ListenBrainzUserFeedback.create()

    fake_responses = [
        dict(feedback=[dict(user_id="FAKE", score=1, recording_mbid=uuid1().hex, created=0)]),
        dict(feedback=[dict(user_id="FAKE", score=1, recording_mbid=uuid1().hex, created=1)]),
    ]

    # 2 pages
    monkeypatch.setattr(collect_listenbrainz_feedback, "get_total_feedback_count", lambda _: 199)
    with get_mock_lb_http(*fake_responses) as mock_get:
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])
        assert result.exit_code == 0
        assert mock_get.call_count == 2

    res = ListenBrainzUserFeedback.select_star()
    assert len(res) == 2
    assert res[0]["username"] == "FAKE"
    assert res[0]["feedback_at"] == utils_.utcfromunixtime(0)
    assert res[1]["feedback_at"] == utils_.utcfromunixtime(1)

    # 1 page
    monkeypatch.setattr(collect_listenbrainz_feedback, "get_total_feedback_count", lambda _: 99)
    with get_mock_lb_http(*fake_responses) as mock_get:
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])
        assert result.exit_code == 0
        assert mock_get.call_count == 1

    res = ListenBrainzUserFeedback.select_star()
    assert len(res) == 1
    assert res[0]["username"] == "FAKE"
    assert res[0]["feedback_at"] == utils_.utcfromunixtime(0)
