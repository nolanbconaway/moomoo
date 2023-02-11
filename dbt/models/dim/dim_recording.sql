
{{ config(
  indexes=[
    {'columns': ['recording_mbid'], 'unique': True},
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
    , {{ json_get('payload_json', p + ["length"]) }}::int as "recording_length_ms"
    , {{ json_get('payload_json', p + ["tag-list"], as_json=True) }} as "tag_list"
    , {{ json_get('payload_json', p + ["release-list"], as_json=True) }} as "release_list"
    , {{ json_get('payload_json', p + ["artist-credit"], as_json=True) }} as "artist_credit_list"
    , {{ json_get('payload_json', p + ["artist-credit-phrase"]) }} as "artist_credit_phrase"
    , "ts_utc" as "_ingest_insert_ts_utc"

  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'recording'
    and {{ json_get('payload_json', ['_success']) }} = 'true'
)

, first_year as (
  select
    "recording_mbid"
    , min(
      substring({{ json_get('release.value', ['date']) }} from 1 for 4)::int
    ) as "release_year"
  
  from t
  , jsonb_array_elements(t.release_list) as release

  where {{ json_get('release.value', ['date']) }} is not null
  group by 1
)

select
  t."recording_mbid"
  , t."recording_title"
  , t."artist_credit_phrase"
  , first_year."release_year"
  , {{ json_get('t.artist_credit_list', [0, 'artist', 'id']) }}::uuid as "artist_mbid"
  , t."recording_length_ms"
  , t."tag_list"
  , t."release_list"
  , t."artist_credit_list"
  , t."_ingest_insert_ts_utc"

from t
inner join first_year on first_year.recording_mbid = t.recording_mbid