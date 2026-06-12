from click.testing import CliRunner
from sqlalchemy.exc import ProgrammingError

from moomoo_pg.cli import cli as db_cli
from moomoo_pg.ddl import TABLES


def test_cli__ddl():
    runner = CliRunner()
    valid_table = TABLES[0].__tablename__
    invalid_table = "nonexistent_table"

    # error if all and table_name are both specified
    res = runner.invoke(db_cli, ["ddl", "--all", valid_table])
    assert res.exit_code != 0
    assert "Must specify either --all or a table name, not both." in res.output

    # error if neither all nor table_name are specified
    res = runner.invoke(db_cli, ["ddl"])
    assert res.exit_code != 0
    assert "Must specify either --all or a table name." in res.output

    # error if table_name is not a valid table name
    res = runner.invoke(db_cli, ["ddl", invalid_table])
    assert res.exit_code != 0
    assert "Error: Invalid value for " in res.output
    assert "'nonexistent_table' is not one of" in res.output

    # print ddl for all tables
    res = runner.invoke(db_cli, ["ddl", "--all"])
    assert res.exit_code == 0
    assert res.stdout.count("CREATE TABLE") == len(TABLES)

    # print ddl for one table
    res = runner.invoke(db_cli, ["ddl", valid_table])
    assert res.exit_code == 0
    assert res.stdout.count("CREATE TABLE") == 1


def test_cli__create():
    runner = CliRunner()
    table = TABLES[0]
    table_name = table.__tablename__
    invalid_table_name = "nonexistent_table"

    # error if table exists and if_not_exists/drop are not specified
    table.create(if_not_exists=True)  # table is autocreated by fixture. this just makes sure.
    assert table.exists()

    res = runner.invoke(db_cli, ["create", table_name])
    assert res.exit_code != 0
    assert isinstance(res.exception, ProgrammingError)
    assert "already exists" in str(res.exception)

    # silently do nothing if table exists and if_not_exists is specified
    assert table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--if-not-exists"])
    assert res.exit_code == 0

    # drop the table
    assert table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--drop"])
    assert res.exit_code == 0

    # silently do nothing if table does not exist and drop is specified
    table.drop()
    assert not table.exists()
    res = runner.invoke(db_cli, ["create", table_name, "--drop"])
    assert res.exit_code == 0

    # error if invalid table name is specified
    res = runner.invoke(db_cli, ["create", invalid_table_name])
    assert res.exit_code != 0
    assert "Error: Invalid value for " in res.output
    assert "'nonexistent_table' is not one of" in res.output
