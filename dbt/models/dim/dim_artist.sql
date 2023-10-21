{{ config(
  indexes=[
    {'columns': ['artist_mbid'], 'unique': True},
  ]
) }}


{# path to most of the data #}
{% set p=["data", "artist"] %}
with dim as (
  select
    "mbid" as "artist_mbid"
    , {{ json_get('payload_json', p + ["name"]) }}::varchar as "artist_name"
    , {{ json_get('payload_json', p + ["type"]) }}::varchar as "artist_type"
    , {{ json_get('payload_json', p + ["alias-list"], as_json=True) }} as "alias_list"
    , {{ json_get('payload_json', p + ["url-relation-list"], as_json=True) }} as "url_relation_list"
    , {{
        json_get('payload_json', p + ["artist-relation-list"], as_json=True)
      }} as "artist_relation_list"
    , "ts_utc"

  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'artist'
    and {{ json_get('payload_json', ['_success']) }} = 'true'
)

, stats_ as (
  select
    "mbid" as "artist_mbid"
    , to_timestamp({{ json_get('payload_json', ['data', 'from_ts']) }}::int) as "from_ts"
    , to_timestamp({{ json_get('payload_json', ['data', 'to_ts']) }}::int) as "to_ts"
    , {{ json_get('payload_json', ['data', 'total_listen_count']) }}::int as "total_listen_count"
    , {{ json_get('payload_json', ['data', 'listeners'], as_json=true) }} as "listeners"
    , "ts_utc"

  from {{ source('pyingest', 'listenbrainz_artist_stats') }}
  where {{ json_get('payload_json', ['success']) }} = 'true'
)

select
  dim.artist_mbid
  , dim.artist_name
  , dim.artist_type
  , dim.alias_list
  , dim.url_relation_list
  , dim.artist_relation_list
  , stats_.from_ts as stats_from_ts
  , stats_.to_ts as stats_to_ts
  , stats_.total_listen_count
  , stats_.listeners
  , dim.ts_utc as annotate_ingest_ts_utc
  , stats_.ts_utc as stats_ingest_ts_utc

from dim
left join stats_ using ("artist_mbid")
