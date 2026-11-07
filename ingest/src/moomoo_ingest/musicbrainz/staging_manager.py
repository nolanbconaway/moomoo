import time
from typing import ClassVar

from sqlalchemy import text
from sqlalchemy.orm import Session

from .ddl import MusicBrainzBase


class StagingSchemaManager:
    PREFIX: ClassVar[str] = "mbz_copy_"

    def __init__(self, session: Session, live_schema: str = "musicbrainz") -> None:
        self.session = session
        self.live_schema = live_schema
        self.ts = int(time.time())
        self.bak_schema = f"{self.PREFIX}bak_{self.ts}"
        self.stg_schema = f"{self.PREFIX}stg_{self.ts}"
        self.promoted = False

    def __enter__(self) -> "StagingSchemaManager":
        self.create_staging()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.cleanup()

    @classmethod
    def drop_orphaned_schemas(cls, session: Session) -> None:
        """Find and drop any leftover schemas from previous (crashed) runs."""
        rows = session.execute(
            text("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname LIKE :pattern"),
            {"pattern": f"{cls.PREFIX}%"},
        )
        for row in rows:
            session.execute(text(f"DROP SCHEMA IF EXISTS {row.nspname} CASCADE"))
        session.commit()

    def create_staging(self) -> None:
        self.session.execute(text(f"DROP SCHEMA IF EXISTS {self.stg_schema} CASCADE"))
        self.session.execute(text(f"CREATE SCHEMA {self.stg_schema}"))
        self.session.commit()

        engine = self.session.get_bind()
        translated_engine = engine.execution_options(
            schema_translate_map={"musicbrainz": self.stg_schema}
        )
        MusicBrainzBase.metadata.create_all(translated_engine)

    def promote(self) -> None:
        self.session.execute(text(f"ALTER SCHEMA {self.live_schema} RENAME TO {self.bak_schema}"))
        self.session.execute(text(f"ALTER SCHEMA {self.stg_schema} RENAME TO {self.live_schema}"))
        self.session.commit()
        self.promoted = True

    def cleanup(self) -> None:
        if not self.promoted:
            self.session.execute(text(f"DROP SCHEMA IF EXISTS {self.stg_schema} CASCADE"))
        else:
            self.session.execute(text(f"DROP SCHEMA IF EXISTS {self.bak_schema} CASCADE"))
        self.session.commit()
