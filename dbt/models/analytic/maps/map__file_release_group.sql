{{ config(
    materialized='table',
    indexes=[{'columns': ['filepath', 'release_group_mbid'], 'unique': True}]
  )
}}

{# Map files to potential recording mbids. #}

select local_files.filepath, map_.release_group_mbid

from {{ ref('local_files') }} as local_files
inner join {{ ref('messybrainz_name_map') }} as map_ using (recording_md5)
where map_.release_group_mbid is not null
  -- exclude a custom library that does not map to anything on musicbrainz
  and local_files.filepath not like 'chopnscrew/%'

union distinct

select filepath, release_group_mbid
from {{ ref('local_files') }}
where release_group_mbid is not null
