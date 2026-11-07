import itertools
import sys

import click
from moomoo_pg import get_musicbrainz_session, get_session
from tqdm import tqdm

from .ddl import MusicBrainzBase
from .mbids import MbidCollection, get_mbids
from .staging_manager import StagingSchemaManager

MODELS = list(MusicBrainzBase.__subclasses__())
TABLE_NAMES: list[str] = [model.__tablename__ for model in MODELS]


def run_copy(batch_size: int, tables: tuple[str, ...] = ()) -> None:
    models = [m for m in MODELS if not tables or m.__tablename__ in tables]
    mbz_session = get_musicbrainz_session()
    app_session = get_session()

    click.echo("fetching mbids")
    all_mbids = get_mbids(app_session, loggerfn=click.echo)

    full_copy_models = [m for m in models if m.is_full_copy]
    scoped_models = [m for m in models if not m.is_full_copy]
    batches = list(itertools.batched(all_mbids, batch_size))

    click.echo("dropping orphaned schemas")
    StagingSchemaManager.drop_orphaned_schemas(app_session)

    with StagingSchemaManager(app_session) as manager:
        click.echo(f"copying {len(full_copy_models)} full-copy tables")
        for model in full_copy_models:
            model.copy(
                mbz_session=mbz_session,
                app_session=app_session,
                mbids=MbidCollection([]),
                dest_schema=manager.stg_schema,
            )
        app_session.commit()

        click.echo(f"copying {len(scoped_models)} id-scoped tables across {len(batches)} batches")
        with tqdm(
            total=len(all_mbids),
            mininterval=0.1 if sys.stderr.isatty() else 10,
            maxinterval=10,
        ) as progress:
            for batch in batches:
                for model in scoped_models:
                    model.copy(
                        mbz_session=mbz_session,
                        app_session=app_session,
                        mbids=MbidCollection(batch),
                        dest_schema=manager.stg_schema,
                    )
                app_session.commit()

                # kill transaction to release any locks; nothing being written to mbz session anyway
                mbz_session.rollback()

                progress.update(len(batch))

        click.echo("promoting staging schema to live")
        manager.promote()
