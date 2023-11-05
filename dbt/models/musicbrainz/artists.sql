{{ config(
  indexes=[
    {'columns': ['artist_mbid'], 'unique': True},
    {'columns': ['artist_name']},
    {'columns': ['_ingest_insert_ts_utc']},
    ]
  ) }}

select
  "mbid" as "artist_mbid"
  , {{ json_get('payload_json', ["data", "artist", "name"]) }}::varchar as "artist_name"
  , {{ json_get('payload_json', ["data", "artist", "type"]) }}::varchar as "artist_type"
  , {{ json_get('payload_json', ["data", "artist", "alias-list"], as_json=True) }} as "alias_list"
  , {{ json_get('payload_json', ["data", "artist", "url-relation-list"], as_json=True) }} as "url_relation_list"
  , {{
      json_get('payload_json', ["data", "artist", "artist-relation-list"], as_json=True)
    }} as "artist_relation_list"
  , "ts_utc" as "_ingest_insert_ts_utc"

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'artist'
  and {{ json_get('payload_json', ['_success']) }} = 'true'
