
{{ config(
  indexes=[
    {'columns': ['artist_mbid'], 'unique': True},
    {'columns': ['_ingest_insert_ts_utc']},
  ]
) }}


{# path to most of the data #}
{% set p=["data", "artist"] %}

select
  "mbid" as "artist_mbid"
  , {{ json_get('payload_json', p + ["name"]) }}::varchar as "artist_name"
  , {{ json_get('payload_json', p + ["type"]) }}::varchar as "artist_type"
  , {{ json_get('payload_json', p + ["alias-list"], as_json=True) }} as "alias_list"
  , {{ json_get('payload_json', p + ["url-relation-list"], as_json=True) }} as "url_relation_list"
  , {{
      json_get('payload_json', p + ["artist-relation-list"], as_json=True)
    }} as "artist_relation_list"
  , "ts_utc" as "_ingest_insert_ts_utc"

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'artist'
  and {{ json_get('payload_json', ['_success']) }} = 'true'
