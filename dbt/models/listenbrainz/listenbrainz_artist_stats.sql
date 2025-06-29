{{ 
  config(
    materialized='table',
    indexes=[
      {'columns': ['artist_mbid'], 'unique': True},
      {'columns': ['stats_range']},
      {'columns': ['artist_name']},
      {'columns': ['total_user_count']},
      {'columns': ['total_listen_count']},
    ]
  )
}}
select
  mbid as artist_mbid
  , {{ json_get('payload_json', ['data', 'stats_range']) }} as stats_range
  , {{ json_get('payload_json', ['data', 'artist_name']) }} as artist_name
  , to_timestamp({{ json_get('payload_json', ['data', 'from_ts']) }}::int) as from_ts
  , to_timestamp({{ json_get('payload_json', ['data', 'to_ts']) }}::int) as to_ts
  , to_timestamp({{ json_get('payload_json', ['data', 'last_updated']) }}::int) as last_updated
  , {{ json_get('payload_json', ['data', 'total_user_count']) }}::int as total_user_count
  , {{ json_get('payload_json', ['data', 'total_listen_count']) }}::int as total_listen_count
  , {{ json_get('payload_json', ['data', 'listeners'], as_json=True) }} as listeners
  , ts_utc as _insert_ts_utc

from {{ source('pyingest', 'listenbrainz_artist_stats') }}
where {{ json_get('payload_json', ['success']) }}::boolean
