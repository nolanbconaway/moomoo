{{ config(
  indexes=[
    {'columns': ['recording_md5'], 'unique': True},
    {'columns': ['recording_name']},
    {'columns': ['artist_name']},
    {'columns': ['recording_mbid']},
    {'columns': ['release_mbid']},
    {'columns': ['insert_ts_utc']},
  ]
) }}

{# Payloads like:
  {
    "artist_mbids": ["084308bd-1654-436f-ba03-df6697104e19"],
    "release_mbid": "fd3c6333-9e3e-4360-aff7-05c0512e8b38",
    "release_name": "Kerplunk!",
    "recording_mbid": "cef5e2a8-0272-4dff-84c6-88721af19b2f",
    "recording_name": "Welcome to Paradise",
    "artist_credit_name": "Green Day"
  }
#}


select
  recording_md5
  , recording_name
  , artist_name
  , {{ json_get('payload_json', ['artist_mbids'], as_json=True) }} as artist_mbids
  , {{ try_cast_uuid(json_get('payload_json', ['recording_mbid'])) }} as recording_mbid
  , {{ try_cast_uuid(json_get('payload_json', ['release_mbid'])) }} as release_mbid
  , {{ json_get('payload_json', ['release_name']) }}::varchar as mapped_release_name
  , {{ json_get('payload_json', ['recording_name']) }}::varchar as mapped_recording_name
  , {{ json_get('payload_json', ['artist_credit_name']) }}::varchar as mapped_artist_name
  , ts_utc as insert_ts_utc

from {{ source('pyingest', 'messybrainz_name_map') }}

where success
