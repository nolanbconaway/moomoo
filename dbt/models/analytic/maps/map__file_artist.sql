{{ config(
    materialized='table',
    indexes=[{'columns': ['filepath', 'artist_mbid'], 'unique': True}]
  )
}}

{# Map files to potential recording mbids. #}

select local_files.filepath, artist_mbid_values.value::uuid as artist_mbid

from {{ ref('local_files') }} as local_files
inner join {{ ref('messybrainz_name_map') }} as map_ using (recording_md5)
, jsonb_array_elements_text(map_.artist_mbids) as artist_mbid_values

where map_.artist_mbids is not null
  and jsonb_array_length(map_.artist_mbids) > 0
  -- exclude a custom library that does not map to anything on musicbrainz
  and local_files.filepath not like 'chopnscrew/%'

union distinct

select filepath, artist_mbid
from {{ ref('local_files') }}
where artist_mbid is not null
