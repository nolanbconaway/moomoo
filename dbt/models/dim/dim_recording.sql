
{{ config(
  indexes=[
    {'columns': ['recording_mbid'], 'unique': True},
    {'columns': ['release_mbid']},
    {'columns': ['release_artist_mbid']},
    {'columns': ['artist_mbid']},    
    {'columns': ['_ingest_insert_ts_utc']},

  ]
) }}

{# path to most of the data #}
{% set p=["data", "recording"] %}

with t as (
  select
    "mbid" as "recording_mbid"
    , {{ json_get('payload_json', p + ["title"]) }} as "recording_title"
    , {{ json_get('payload_json', p + ["length"]) }}::int as "recording_length"
    , {{ json_get('payload_json', p + ["tag-list"], as_json=True) }} as "tag_list"
    , {{ json_get('payload_json', p + ["release-list"], as_json=True) }} as "release_list"
    , {{ json_get('payload_json', p + ["artist-credit"], as_json=True) }} as "artist_credit_list"
    , {{ json_get('payload_json', p + ["artist-credit-phrase"]) }} as "artist_credit_phrase"
    , "ts_utc" as "_ingest_insert_ts_utc"

  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'recording'
    and {{ json_get('payload_json', ['_success']) }} = 'true'
)

select
  "recording_mbid"
  , "recording_title"
  , "artist_credit_phrase"
  , substring({{ json_get('release_list', [0, 'date']) }} from 1 for 4)::int as "release_year"
  , {{ json_get('release_list', [0, 'id']) }}::uuid as "release_mbid"
  , {{
      json_get('release_list', [0, "artist-credit", 0 , 'artist', 'id']) 
    }}::uuid as "release_artist_mbid"
  , {{ json_get('artist_credit_list', [0, 'artist', 'id']) }}::uuid as "artist_mbid"
  , "recording_length"
  , "tag_list"
  , "release_list"
  , "artist_credit_list"
  , "_ingest_insert_ts_utc"

from t
