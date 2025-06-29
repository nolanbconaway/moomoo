{{ config(
  indexes=[
    {'columns': ['release_group_mbid'], 'unique': True},
    {'columns': ['release_group_title']},
    {'columns': ['artist_credit_phrase']},
    {'columns': ['_ingest_insert_ts_utc']},
  ]
) }}

select
  mbid as release_group_mbid
  , {{ json_get('payload_json', ["data", "release-group", "title"]) }} as release_group_title
  , {{ extract_year(
      json_get('payload_json', ["data", "release-group", "first-release-date"])
    ) }} as release_group_year
  , {{ json_get('payload_json', ["data", "release-group", "artist-credit"], as_json=True) }} as artist_credit_list
  , {{ json_get('payload_json', ["data", "release-group", "tag-list"], as_json=True) }} as tag_list
  , {{ json_get('payload_json', ["data", "release-group", "url-relation-list"], as_json=True) }} as url_relation_list
  , {{ json_get('payload_json', ["data", "release-group", "artist-credit-phrase"]) }} as artist_credit_phrase
  , ts_utc as _ingest_insert_ts_utc

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'release-group'
  and {{ json_get('payload_json', ['_success']) }} = 'true'
