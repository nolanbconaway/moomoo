{{
  config(
    materialized='incremental',
    unique_key="release_group_mbid",
    indexes=[
      {'columns': ['release_group_mbid'], 'unique': True},
      {'columns': ['release_group_title']},
      {'columns': ['artist_credit_phrase']},
      {'columns': ['_ingest_insert_ts_utc']},
    ]
  )
}}

select
  mbid as release_group_mbid
  , {{ json_get('payload_json', ["data", "release-group", "title"]) }} as release_group_title
  , {{ json_get('payload_json', ["data", "release-group", "release-count"]) }} as release_count
  , {{ extract_year(
      json_get('payload_json', ["data", "release-group", "first-release-date"])
    ) }} as release_group_year
  , {{ json_get('payload_json', ["data", "release-group", "first-release-date"]) }} as first_release_date
  , {{ json_get('payload_json', ["data", "release-group", "type"]) }} as release_group_type
  , {{ json_get('payload_json', ["data", "release-group", "primary-type"]) }} as release_group_primary_type
  , {{ json_get('payload_json', ["data", "release-group", "artist-credit"], as_json=True) }} as artist_credit_list
  , {{ json_get('payload_json', ["data", "release-group", "tag-list"], as_json=True) }} as tag_list
  , {{ json_get('payload_json', ["data", "release-group", "url-relation-list"], as_json=True) }} as url_relation_list
  , {{ json_get('payload_json', ["data", "release-group", "artist-credit-phrase"]) }} as artist_credit_phrase
  , ts_utc as _ingest_insert_ts_utc

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'release-group'
  and {{ json_get('payload_json', ['_success']) }} = 'true'

-- noqa: disable=all
  {% if is_incremental() %}
    and ts_utc > (
      select max(t._ingest_insert_ts_utc) - interval '5 minutes' from {{ this }} as t
    )
  {% endif %}
-- noqa: enable=all
