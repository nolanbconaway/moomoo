{{
  config(
    materialized='incremental',
    unique_key='recording_mbid',
    indexes=[
      {'columns': ['recording_mbid'], 'unique': True},
      {'columns': ['recording_title']},
      {'columns': ['artist_credit_phrase']},
      {'columns': ['_ingest_insert_ts_utc']},
    ]
  )
}}

select
  mbid as recording_mbid
  , {{ json_get('payload_json', ["data", "recording", "title"]) }} as recording_title
  , {{ json_get('payload_json', ["data", "recording", "length"]) }}::int as recording_length_ms
  , {{ json_get('payload_json', ["data", "recording", "tag-list"], as_json=True) }} as tag_list
  , {{ json_get('payload_json', ["data", "recording", "release-list"], as_json=True) }} as release_list
  , {{ json_get('payload_json', ["data", "recording", "artist-credit"], as_json=True) }} as artist_credit_list
  , {{ json_get('payload_json', ["data", "recording", "artist-credit-phrase"]) }} as artist_credit_phrase
  , ts_utc as _ingest_insert_ts_utc

from {{ source('pyingest', 'musicbrainz_annotations') }}

where entity = 'recording'
  and {{ json_get('payload_json', ['_success']) }} = 'true'

  {% if is_incremental() %} -- noqa: LT02
    and ts_utc > (
      select max(t._ingest_insert_ts_utc) - interval '5 minutes' from {{ this }} as t
    )
  {% endif %}
