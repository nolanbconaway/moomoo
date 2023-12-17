{{ config(
    materialized='table',
    indexes=[{'columns': ['release_group_mbid', 'artist_mbid'], 'unique': True}]
  )
}}

select distinct -- found a dupe once
  release_groups.release_group_mbid
  , {{ try_cast_uuid(json_get('artist_credits.value', ["artist", "id"])) }} as artist_mbid

from {{ ref('release_groups') }} as release_groups
, jsonb_array_elements(release_groups.artist_credit_list) as artist_credits

where {{ try_cast_uuid(json_get('artist_credits.value', ["artist", "id"])) }} is not null
