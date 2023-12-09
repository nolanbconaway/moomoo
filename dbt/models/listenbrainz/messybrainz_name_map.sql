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
  "metadata": {
    "release": {
      "mbid": "8549ce34-2349-42e0-bf28-1a773b35bcaa",
      "name": "The Soft Bulletin",
      "year": 1999,
      "caa_id": 23592292976,
      "caa_release_mbid": "8549ce34-2349-42e0-bf28-1a773b35bcaa",
      "album_artist_name": "The Flaming Lips",
      "release_group_mbid": "1a021034-95d1-3a2d-bdca-73b25e455e49"
    },
    "recording": {
      "name": "A Spoonful Weighs a Ton",
      "rels": [],
      "length": 212000
    }
  },
  "artist_mbids": ["1f43d76f-8edf-44f6-aaf1-b65f05ad9402"],
  "release_mbid": "8549ce34-2349-42e0-bf28-1a773b35bcaa",
  "release_name": "The Soft Bulletin",
  "recording_mbid": "6deeee64-d457-4c72-be51-9d387dafecec",
  "recording_name": "A Spoonful Weighs a Ton",
  "artist_credit_name": "The Flaming Lips"
}
#}


select
  recording_md5
  , recording_name
  , artist_name
  , {{ json_get('payload_json', ['artist_mbids'], as_json=True) }} as artist_mbids
  , {{ try_cast_uuid(json_get('payload_json', ['recording_mbid'])) }} as recording_mbid
  , {{ try_cast_uuid(json_get('payload_json', ['release_mbid'])) }} as release_mbid
  , (
    {{ try_cast_uuid(json_get('payload_json', ['metadata', 'release', 'release_group_mbid'])) }}
  ) as release_group_mbid
  , {{ json_get('payload_json', ['release_name']) }}::varchar as mapped_release_name
  , {{ json_get('payload_json', ['recording_name']) }}::varchar as mapped_recording_name
  , {{ json_get('payload_json', ['artist_credit_name']) }}::varchar as mapped_artist_name
  , ts_utc as insert_ts_utc

from {{ source('pyingest', 'messybrainz_name_map') }}

where success
