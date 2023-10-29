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
    t.release_mbid
    , array_agg({{ json_get('ac.value', ['artist', 'id']) }}::uuid) as "artist_mbids_list"

  from t
  , jsonb_array_elements("artist_credit_list") as ac -- noqa: AL05

  where ac.value is not null
  group by 1
)

select
  t."release_mbid"
  , t."release_title"
  , t."artist_credit_phrase"
  , substring(t."release_date" from 1 for 4)::int as "release_year"
  , {{ json_get('artist_credit_list', [0,  'artist', 'id']) }}::uuid as "artist_mbid"
  , t."release_status"
  , t."release_barcode"
  , t."release_country"
  , t."release_asin"
  , t."release_date"
  , t."artist_credit_list"
  , artist_array."artist_mbids_list" as "artist_mbids_list"
  , t."label_info_list"
  , t."url_relation_list"
  , t."_ingest_insert_ts_utc"

from t
left join artist_array on t.release_mbid = artist_array.release_mbid
