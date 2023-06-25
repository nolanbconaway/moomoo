from click.testing import CliRunner

from moomoo import utils_
from moomoo.ml import score_local_files

from ..conftest import RESOURCES


def create_table(schema, table):
    utils_.create_table(schema=schema, table=table, ddl=score_local_files.DDL)


def test_main__new_table():
    runner = CliRunner()
    create_table(schema="test", table="fake")
    result = runner.invoke(
        score_local_files.main, [str(RESOURCES), "--table=fake", "--schema=test"]
    )
    assert result.exit_code == 0
    assert "Scoring" in result.output
    res = utils_.execute_sql_fetchall("select * from test.fake")
    assert len(res) == 1


def test_main__skip_already_scored():
    create_table(schema="test", table="fake")
    with utils_.pg_connect() as conn:
        with conn.cursor() as cur:
            sql = f"""
                insert into test.fake (
                    filepath, success, fail_reason, duration_seconds, embedding
                )
                values ('test.mp3', false, 'uhoh', null, null)
            """
            cur.execute(sql)
            conn.commit()

    runner = CliRunner()
    result = runner.invoke(
        score_local_files.main, [str(RESOURCES), "--table=fake", "--schema=test"]
    )

    assert result.exit_code == 0
    assert "Nothing to do" in result.output

    res = utils_.execute_sql_fetchall("select * from test.fake")
    assert len(res) == 1
    assert res[0]["filepath"] == "test.mp3"
    assert res[0]["success"] is False
    assert res[0]["fail_reason"] == "uhoh"
