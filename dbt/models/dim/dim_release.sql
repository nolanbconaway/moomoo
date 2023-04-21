
{{ config(
  indexes=[
    {'columns': ['release_mbid'], 'unique': True},
    {'columns': ['artist_mbid']},
    {'columns': ['_ingest_insert_ts_utc']},
  ]
) }}

{# path to most of the data #}
{% set p=["data", "release"] %}

with t as (
  select
    "mbid" as "release_mbid"
    , {{ json_get('payload_json', p + ["asin"]) }} as "release_asin"
    , {{ json_get('payload_json', p + ["date"]) }} as "release_date"
    , {{ json_get('payload_json', p + ["title"]) }} as "release_title"
    , {{ json_get('payload_json', p + ["status"]) }} as "release_status"
    , {{ json_get('payload_json', p + ["barcode"]) }} as "release_barcode"
    , {{ json_get('payload_json', p + ["country"]) }} as "release_country"
    , {{ json_get('payload_json', p + ["artist-credit"], as_json=True) }} as "artist_credit_list"
    , {{ json_get('payload_json', p + ["label-info-list"], as_json=True) }} as "label_info_list"
    , {{ json_get('payload_json', p + ["url-relation-list"], as_json=True) }} as "url_relation_list"
    , {{ json_get('payload_json', p + ["artist-credit-phrase"]) }} as "artist_credit_phrase"
    , "ts_utc" as "_ingest_insert_ts_utc"

  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'release'
    and {{ json_get('payload_json', ['_success']) }} = 'true'
)

, artist_array as (
  select
    release_mbid
    , array_agg({{ json_get('artist_credit', ['artist', 'id']) }}::uuid) as "artist_mbids_list"

  from t
    , jsonb_array_elements("artist_credit_list") as artist_credit

  where "artist_credit" is not null
  group by 1
)

select
  t."release_mbid"
  , "release_title"
  , "artist_credit_phrase"
  , substring("release_date" from 1 for 4)::int as "release_year"
  , {{ json_get('artist_credit_list', [0,  'artist', 'id']) }}::uuid as "artist_mbid"
  , "release_status"
  , "release_barcode"
  , "release_country"
  , "release_asin"
  , "release_date"
  , "artist_credit_list"
  , artist_array."artist_mbids_list" as "artist_mbids_list"
  , "label_info_list"
  , "url_relation_list"
  , "_ingest_insert_ts_utc"

from t
left join artist_array on artist_array.release_mbid = t.release_mbid
