import dataclasses
import json
from collections.abc import Callable
from hashlib import md5
from uuid import UUID

from moomoo_pg import (
    ListenBrainzListen,
    ListenBrainzSimilarUserActivity,
    ListenBrainzUserFeedback,
    LocalFile,
    MessyBrainzNameMap,
    execute_sql_fetchall,
)
from sqlalchemy.orm import Session

from ..annotate_mbids import Mbid
from ..utils_ import ENTITIES


@dataclasses.dataclass
class MbidCollection:
    mbids: list[Mbid]
    collection_hash: str = dataclasses.field(init=False)
    by_entity: dict[str, list[UUID]] = dataclasses.field(init=False)

    def __post_init__(self):
        self.mbids = sorted(self.mbids, key=lambda item: (item.entity, str(item.mbid)))
        self.collection_hash = md5(
            json.dumps([str(item.mbid) for item in self.mbids]).encode()
        ).hexdigest()

        self.by_entity = {entity: [] for entity in ENTITIES}
        for item in self.mbids:
            self.by_entity[item.entity].append(item.mbid)


def query_mbids(sql: str, session: Session) -> list[Mbid]:
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""
    mbids = []
    for row in execute_sql_fetchall(sql=sql, session=session):
        try:
            mbid = UUID(str(row["mbid"]))
        except (TypeError, ValueError):
            continue
        mbids.append(Mbid(mbid=mbid, entity=row["entity"]))
    return mbids


def get_listen_mbids(session: Session) -> list[Mbid]:
    sql = f"""
        select distinct ids.mbid, ids.entity
        from {ListenBrainzListen.table_name()} as listens
        cross join lateral (
            select
                artist_mbids.mbid as mbid,
                'artist' as entity
            from jsonb_array_elements_text(
                listens.json_data #> '{{track_metadata,mbid_mapping,artist_mbids}}'
            ) as artist_mbids(mbid)

            union all

            select
                artist_mbids.mbid as mbid,
                'artist' as entity
            from jsonb_array_elements_text(
                listens.json_data #> '{{track_metadata,additional_info,artist_mbids}}'
            ) as artist_mbids(mbid)

            union all

            select
                listens.json_data #>> '{{track_metadata,additional_info,artist_mbid}}' as mbid,
                'artist' as entity

            union all

            select
                coalesce(
                    listens.json_data #>> '{{track_metadata,mbid_mapping,recording_mbid}}',
                    listens.json_data #>> '{{track_metadata,additional_info,recording_mbid}}'
                ) as mbid,
                'recording' as entity

            union all

            select
                coalesce(
                    listens.json_data #>> '{{track_metadata,mbid_mapping,release_mbid}}',
                    listens.json_data #>> '{{track_metadata,additional_info,release_mbid}}'
                ) as mbid,
                'release' as entity

            union all

            select
                listens.json_data #>> '{{track_metadata,mbid_mapping,release_group_mbid}}' as mbid,
                'release-group' as entity

            union all

            select
                (
                    listens.json_data #>> '{{track_metadata,additional_info,release_group_mbid}}'
                ) as mbid,
                'release-group' as entity
        ) as ids(mbid, entity)
        where ids.mbid is not null
    """
    return query_mbids(sql, session)
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""


def get_local_file_mbids(session: Session) -> list[Mbid]:
    sql = f"""
        select distinct ids.mbid, ids.entity
        from {LocalFile.table_name()} as files
        cross join lateral (
            values
                (files.json_data ->> 'musicbrainz_albumid', 'release'),
                (files.json_data ->> 'musicbrainz_releasegroupid', 'release-group'),
                (files.json_data ->> 'musicbrainz_trackid', 'recording'),
                (files.json_data ->> 'musicbrainz_artistid', 'artist'),
                (files.json_data ->> 'musicbrainz_albumartistid', 'artist')
        ) as ids(mbid, entity)
        where ids.mbid is not null
    """
    return query_mbids(sql, session)
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""


def get_messybrainz_mbids(session: Session) -> list[Mbid]:
    sql = f"""
        select distinct ids.mbid, ids.entity
        from {MessyBrainzNameMap.table_name()} as mappings
        cross join lateral (
            select artist_mbids.mbid, 'artist' as entity
            from jsonb_array_elements_text(
                mappings.payload_json -> 'artist_mbids'
            ) as artist_mbids(mbid)

            union all

            select mappings.payload_json ->> 'recording_mbid', 'recording'

            union all

            select mappings.payload_json ->> 'release_mbid', 'release'
        ) as ids(mbid, entity)
        where ids.mbid is not null
    """
    return query_mbids(sql, session)
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""


def get_similar_user_activity_mbids(session: Session) -> list[Mbid]:
    sql = f"""
        select distinct ids.mbid, ids.entity
        from {ListenBrainzSimilarUserActivity.table_name()} as activity
        cross join lateral jsonb_array_elements(
            case activity.entity
                when 'artists' then activity.json_data -> 'artists'
                when 'releases' then activity.json_data -> 'releases'
                when 'recordings' then activity.json_data -> 'recordings'
            end
        ) as item(value)
        cross join lateral (
            values (
                case activity.entity
                    when 'artists' then item.value ->> 'artist_mbid'
                    when 'releases' then item.value ->> 'release_mbid'
                    when 'recordings' then item.value ->> 'recording_mbid'
                end,
                case activity.entity
                    when 'artists' then 'artist'
                    when 'releases' then 'release'
                    when 'recordings' then 'recording'
                end
            )
        ) as ids(mbid, entity)
        where ids.mbid is not null
    """
    return query_mbids(sql, session)
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""


def get_feedback_mbids(session: Session) -> list[Mbid]:
    sql = f"""
        select distinct recording_mbid as mbid, 'recording' as entity
        from {ListenBrainzUserFeedback.table_name()}
    """
    return query_mbids(sql, session)
    """Run a query which exports a mbid and entity column, get back a List of Mbid objects."""


def get_mbids(session: Session, loggerfn: Callable[[str], None] | None = None) -> list[Mbid]:
    if loggerfn is None:
        loggerfn = lambda message: None

    mbids = []
    for source_name, get_source_mbids in (
        ("listens", get_listen_mbids),
        ("local files", get_local_file_mbids),
        ("MessyBrainz", get_messybrainz_mbids),
        ("similar user activity", get_similar_user_activity_mbids),
        ("ListenBrainz feedback", get_feedback_mbids),
    ):
        source_mbids = get_source_mbids(session)
        loggerfn(f"{source_name}: {len(source_mbids)} MBIDs")
        mbids.extend(source_mbids)

    unique_mbids = sorted(set(mbids), key=lambda mbid: (mbid.entity, str(mbid.mbid)))
    loggerfn(f"Total deduplicated MBIDs: {len(unique_mbids)}")
    return unique_mbids
