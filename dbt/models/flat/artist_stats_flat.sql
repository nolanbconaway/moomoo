
{{ config(
  indexes=[
    {'columns': ['mbid'], 'unique': True},
    {'columns': ['ts_utc']},
  ]
) }}

{#
Sample payload:

{
    "data": {
        "to_ts": 1689120001,
        "from_ts": 1009843200,
        "listeners": [
            {
                "user_name": "...",
                "listen_count": 1603
            },
            ...
        ],
        "artist_mbid": "...",
        "artist_name": "...",
        "stats_range": "all_time",
        "last_updated": 1689143891,
        "total_listen_count": 8459
    },
    "error": null,
    "success": true
}
#}

with t as (
  select
    "mbid"
    , to_timestamp({{ json_get('payload_json', ['data', 'from_ts']) }}::int) as "from_ts"
    , to_timestamp({{ json_get('payload_json', ['data', 'to_ts']) }}::int) as "to_ts"
    , to_timestamp({{ json_get('payload_json', ['data', 'last_updated']) }}::int) as "lb_last_updated"
    , {{ json_get('payload_json', ['data', 'artist_name']) }}::varchar as "artist_name"
    , {{ json_get('payload_json', ['data', 'stats_range']) }}::varchar as "stats_range"
    , {{ json_get('payload_json', ['data', 'total_listen_count']) }}::int as "total_listen_count"
    , {{ json_get('payload_json', ['data', 'listeners'], as_json=true) }} as "listeners"
    , "ts_utc"

  from {{ source('pyingest', 'listenbrainz_artist_stats') }}
  where {{ json_get('payload_json', ['success']) }} = 'true'
)

select
  "mbid"
  , "from_ts"
  , "to_ts"
  , "lb_last_updated"
  , "artist_name"
  , "stats_range"
  , "total_listen_count"
  , "listeners"
  , "ts_utc"

from t