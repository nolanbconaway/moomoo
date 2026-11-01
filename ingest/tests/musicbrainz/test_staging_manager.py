from uuid import uuid4

from moomoo_pg import get_session
from sqlalchemy import text

from moomoo_ingest.musicbrainz.staging_manager import StagingSchemaManager


def schema_exists(session, schema: str) -> bool:
    return session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema)"),
        {"schema": schema},
    ).scalar_one()


def test_context_creates_staging_schema_and_tables_then_drops_it():
    with get_session() as session:
        manager = StagingSchemaManager(session)

        with manager:
            assert schema_exists(session, manager.stg_schema)
            assert session.execute(
                text("SELECT to_regclass(:table_name) IS NOT NULL"),
                {"table_name": f"{manager.stg_schema}.artist"},
            ).scalar_one()

        assert not schema_exists(session, manager.stg_schema)


def test_promote_swaps_live_and_staging_schemas_and_cleanup_drops_backup():
    live_schema = f"musicbrainz_test_{uuid4().hex}"
    with get_session() as session:
        session.execute(text(f"CREATE SCHEMA {live_schema}"))
        session.commit()
        manager = StagingSchemaManager(session, live_schema=live_schema)

        with manager:
            manager.promote()
            assert schema_exists(session, live_schema)
            assert schema_exists(session, manager.bak_schema)
            assert not schema_exists(session, manager.stg_schema)

        assert not schema_exists(session, manager.bak_schema)
        assert session.execute(
            text("SELECT to_regclass(:table_name) IS NOT NULL"),
            {"table_name": f"{live_schema}.artist"},
        ).scalar_one()


def test_drop_orphaned_schemas_removes_matching_schema():
    orphaned_schema = f"mbz_copy_test_{uuid4().hex}"
    unrelated_schema = f"unrelated_test_{uuid4().hex}"
    with get_session() as session:
        session.execute(text(f"CREATE SCHEMA {orphaned_schema}"))
        session.execute(text(f"CREATE SCHEMA {unrelated_schema}"))
        session.commit()

        StagingSchemaManager.drop_orphaned_schemas(session)

        assert not schema_exists(session, orphaned_schema)
        assert schema_exists(session, unrelated_schema)