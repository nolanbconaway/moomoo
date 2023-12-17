{{ config(
    materialized='table',
    indexes=[{'columns': ['recording_mbid', 'release_mbid'], 'unique': True}]
  )
}}

select
  recordings.recording_mbid
  , {{ try_cast_uuid(json_get('release_list.value', ["id"])) }} as release_mbid

from {{ ref('recordings') }} as recordings
, jsonb_array_elements(recordings.release_list) as release_list
