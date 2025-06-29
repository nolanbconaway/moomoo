{{ config(
  indexes=[
    {'columns': ['similar_user_activity_id'], 'unique': True},
    {'columns': ['ingest_payload_id']},
    {'columns': ['mbid']},
    {'columns': ['from_username']},
    {'columns': ['to_username']},
    {'columns': ['time_range']},
    {'columns': ['entity']},
  ]
) }}

{#
Sample payload:

{
    "count": 53,
    "range": "month",
    "to_ts": 1677628800,
    "offset": 0,
    "from_ts": 1675209600,
    "user_id": "...",
    "releases": [
        {
            "caa_id": 31771080556,
            "artist_name": "Beach House",
            "artist_mbids": [
                "d5cc67b8-1cc4-453b-96e8-44487acdebea"
            ],
            "listen_count": 18,
            "release_mbid": "c36c722d-6b30-4a5d-8f74-ef0c25f5108d",
            "release_name": "Once Twice Melody",
            "caa_release_mbid": "c36c722d-6b30-4a5d-8f74-ef0c25f5108d"
        }
        ...
    ],
    "last_updated": 1678587108,
    "total_release_count": 53
}#}

with exploded as (
  select
    base.payload_id
    , case base.entity
      when 'artists' then {{ json_get('rows_.value', ['artist_mbid']) }}
      when 'releases' then {{ json_get('rows_.value', ['release_mbid']) }}
      when 'recordings' then {{ json_get('rows_.value', ['recording_mbid']) }}
    end as mbid
    , sum({{ json_get('rows_.value', ['listen_count']) }}::int) as listen_count

  from {{ source('pyingest', 'listenbrainz_similar_user_activity') }} as base
  ,
    jsonb_array_elements(
      case base.entity
        when 'artists' then {{ json_get('base.json_data', ['artists'], as_json=True) }}
        when 'releases' then {{ json_get('base.json_data', ['releases'], as_json=True) }}
        when 'recordings' then {{ json_get('base.json_data', ['recordings'], as_json=True) }}
      end
    ) as rows_

  where base.entity in ('artists', 'releases', 'recordings')

  group by 1, 2  -- found some cases of duplicate mbids in the same payload.
)

select
  {{ dbt_utils.generate_surrogate_key(['base.payload_id', 'exploded.mbid']) }} as similar_user_activity_id
  , exploded.mbid::uuid as mbid
  , exploded.listen_count

  , base.from_username
  , base.to_username
  , base.user_similarity

  , base.time_range
  , to_timestamp({{ json_get('base.json_data', ['from_ts']) }}::int) as activity_from_ts
  , to_timestamp({{ json_get('base.json_data', ['to_ts']) }}::int) as activity_to_ts

  -- rename entity for consistency with other tables
  , case base.entity
    when 'artists' then 'artist'
    when 'releases' then 'release'
    when 'recordings' then 'recording'
  end as entity

  , base.payload_id as ingest_payload_id
  , base.insert_ts_utc

from {{ source('pyingest', 'listenbrainz_similar_user_activity') }} as base
inner join exploded on exploded.payload_id = base.payload_id

where exploded.mbid is not null
