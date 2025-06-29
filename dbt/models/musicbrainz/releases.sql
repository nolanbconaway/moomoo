{{ config(
  indexes=[
    {'columns': ['release_mbid'], 'unique': True},
    {'columns': ['release_title']},
    {'columns': ['release_group_mbid']},
    {'columns': ['artist_credit_phrase']},
    {'columns': ['_ingest_insert_ts_utc']},
  ]
) }}

select
  mbid as release_mbid
  , {{ json_get('payload_json', ["data", "release", "title"]) }} as release_title
  , {{ try_cast_uuid(json_get('payload_json', ["data", "release", "release-group", "id"])) }} as release_group_mbid
  , {{ extract_year(json_get('payload_json', ["data", "release", "date"])) }} as release_year
  , {{ json_get('payload_json', ["data", "release", "artist-credit"], as_json=True) }} as artist_credit_list
  , {{ json_get('payload_json', ["data", "release", "label-info-list"], as_json=True) }} as label_info_list
  , {{ json_get('payload_json', ["data", "release", "url-relation-list"], as_json=True) }} as url_relation_list
  , {{ json_get('payload_json', ["data", "release", "artist-credit-phrase"]) }} as artist_credit_phrase
  , ts_utc as _ingest_insert_ts_utc

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'release'
  and {{ json_get('payload_json', ['_success']) }} = 'true'
