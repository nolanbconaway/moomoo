{{ config(
  indexes=[
    {'columns': ['listen_md5'], 'unique': True},
    {'columns': ['username']},
    {'columns': ['listen_at_ts_utc']},
    {'columns': ['recording_msid']},
    {'columns': ['recording_mbid']},
    {'columns': ['release_mbid']},
  ]
) }}

{#
  Unpack JSON to rows. Most other models will be aggregates on this.

  Sample payload:

    {
      "user_name": "...",
      "inserted_at": 1673060956,
      "listened_at": 1673060842,
      "recording_msid": "663dc7d3-5990-4460-a1d9-b29c2db98b66",
      "track_metadata": {
        "track_name": "Hard Dreaming Man",
        "artist_name": "Drugdealer",
        "mbid_mapping": {
          "caa_id": 33932324688,
          "artists": [
            {
              "artist_mbid": "d3b69dfc-4b20-440c-bd23-6f939d076082",
              "join_phrase": "",
              "artist_credit_name": "Drugdealer"
            }
          ],
          "artist_mbids": [
            "d3b69dfc-4b20-440c-bd23-6f939d076082"
          ],
          "release_mbid": "f65e49dd-769e-42df-b8e1-c827f56ac909",
          "recording_mbid": "6cc561d1-3647-4fc5-bbc4-b841ca4c752c",
          "caa_release_mbid": "13cab1ba-dc4e-42ce-9df9-0e0ca52900aa"
        },
        "release_name": "Hiding in Plain Sight",
        "additional_info": {
          "duration_ms": 216085,
          "tracknumber": 8,
          "media_player": "strawberry",
          "recording_msid": "663dc7d3-5990-4460-a1d9-b29c2db98b66",
          "submission_client": "strawberry",
          "media_player_version": "1.0.7",
          "submission_client_version": "1.0.7"
        }
      }
    }
#}

with t as (
  select
    "listen_md5"
    , "username"::varchar as "username"
    , to_timestamp({{ json_get('json_data', ['listened_at']) }}::int) as "listen_at_ts_utc"
    , {{ json_get('json_data', ['recording_msid']) }}::uuid as "recording_msid"
    , {{ json_get('json_data', ['track_metadata', 'track_name']) }}::varchar as "track_name"

    , {{
        json_get('json_data', ['track_metadata', 'artist_name'])
      }}::varchar as "artist_name"

    , {{
        json_get('json_data', ['track_metadata', 'release_name'])
      }}::varchar as "release_name"

    , {{
        json_get('json_data', ['track_metadata', 'mbid_mapping', 'release_mbid'])
      }}::uuid as "release_mbid"

    , {{
        json_get('json_data', ['track_metadata', 'mbid_mapping', 'recording_mbid'])
      }}::uuid as "recording_mbid"

    , {{
        json_get(
          'json_data'
          , ['track_metadata', 'mbid_mapping', 'artist_mbids']
          , as_json=True
        )
      }} as "artist_mbids"

    , {{
        json_get('json_data', ['track_metadata', 'mbid_mapping', 'caa_release_mbid'])
      }}::uuid as "caa_release_mbid"

    , {{
        json_get('json_data', ['track_metadata', 'additional_info', 'duration_ms'])
      }}::int as "duration_ms"

    , {{
        json_get('json_data', ['track_metadata', 'additional_info', 'tracknumber'])
      }}::int as "tracknumber"

    , to_timestamp({{ json_get('json_data', ['inserted_at']) }}::int) as "_lb_insert_ts_utc"
    , "insert_ts_utc" as "_ingest_insert_ts_utc"

  from {{ source('pyingest', 'listenbrainz_listens') }}
)


select
  listen_md5
  , username
  , listen_at_ts_utc
  , recording_msid
  , track_name
  , artist_name
  , release_name
  , release_mbid
  , recording_mbid
  , artist_mbids
  , caa_release_mbid
  , duration_ms
  , tracknumber
  , _lb_insert_ts_utc
  , _ingest_insert_ts_utc

from t
