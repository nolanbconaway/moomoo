{{ config(
    materialized='table',
    indexes=[{'columns': ['filepath', 'recording_mbid'], 'unique': True}]
  )
}}

{# Map files to potential recording mbids. #}

select local_files.filepath, map_.recording_mbid

from {{ ref('local_files') }} as local_files
inner join {{ ref('messybrainz_name_map') }} as map_ using (recording_md5)
where map_.recording_mbid is not null
  -- exclude a custom library that does not map to anything on musicbrainz
  and local_files.filepath not like 'chopnscrew/%'

union distinct

select filepath, recording_mbid
from {{ ref('local_files') }}
where recording_mbid is not null
