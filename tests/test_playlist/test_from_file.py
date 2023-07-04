import tempfile
import uuid
from pathlib import Path
from typing import List

from click.testing import CliRunner

from moomoo import utils_
from moomoo.playlist import playlist_from_file


def load_local_files_table(schema: str, data: List[dict]):
    """Load the local files table with keys:
    - filepath: str
    - embedding_success: bool
    - embedding: list[float]
    - artist_mbid: uuid
    - embedding_duration_seconds: int
    """

    with utils_.pg_connect() as conn:
        cur = conn.cursor()
        sql = f"""
            create table {schema}.local_files_flat (
                filepath text primary key
                , embedding_success bool
                , embedding vector
                , artist_mbid uuid
                , embedding_duration_seconds int
            )
        """
        cur.execute(sql)

        sql = f"""
            insert into {schema}.local_files_flat (
                filepath
                , embedding_success
                , embedding
                , artist_mbid
                , embedding_duration_seconds
            )
            values (
                %(filepath)s
                , %(embedding_success)s
                , %(embedding)s
                , %(artist_mbid)s
                , %(embedding_duration_seconds)s
            )
        """
        for row in data:
            cur.execute(sql, row)
        conn.commit()


def test_file_not_found_errors():
    load_local_files_table(schema="test", data=[])
    runner = CliRunner()

    # does not exist at all
    result = runner.invoke(playlist_from_file.cli, ["--schema=test", "fake.mp3"])
    assert result.exit_code != 0
    assert "'fake.mp3' does not exist." in result.output

    # exists but not in database at all
    with tempfile.NamedTemporaryFile() as f:
        Path(f.name).touch()
        result = runner.invoke(playlist_from_file.cli, ["--schema=test", f.name])
        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)
        assert f"Could not find any matches to {f.name} in database." in str(
            result.exception
        )


def test_no_embedding_error():
    runner = CliRunner()
    with tempfile.NamedTemporaryFile() as f:
        row = dict(
            filepath=f.name,
            embedding_success=False,
            embedding=None,
            artist_mbid=None,
            embedding_duration_seconds=None,
        )
        load_local_files_table(schema="test", data=[row])

        result = runner.invoke(playlist_from_file.cli, ["--schema=test", f.name])
        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)
        assert f"{f.name} has no embeddings in the database." in str(result.exception)


def test_working():
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        # make 10 files in a test directory
        target_dir = Path(tmpdir) / "test"
        target_dir.mkdir()

        rows = []
        for i in range(10):
            (target_dir / str(i)).touch()
            row = dict(
                filepath=f"test/{i}",  # db has local path only
                embedding_success=True,
                embedding=str([i] * 10),
                artist_mbid=uuid.uuid1(),
                embedding_duration_seconds=90,
            )
            rows.append(row)

        load_local_files_table(schema="test", data=rows)

        # works with file input
        result = runner.invoke(
            playlist_from_file.cli, ["--schema=test", str(target_dir / "1")]
        )

        assert result.exit_code == 0
        xml_str = [i for i in result.output.split("\n") if i][-1]
        assert (
            f"<annotation>moomoo generated from-file: {target_dir / '1'}</annotation>"
            in xml_str
        )

        # works with folder input
        result = runner.invoke(
            playlist_from_file.cli, ["--schema=test", str(target_dir)]
        )

        assert result.exit_code == 0
        xml_str = [i for i in result.output.split("\n") if i][-1]
        assert (
            f"<annotation>moomoo generated from-file: {target_dir}</annotation>"
            in xml_str
        )
