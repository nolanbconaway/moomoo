{{ config(
    materialized='table',
    indexes=[{'columns': ['filepath', 'release_group_mbid'], 'unique': True}]
  )
}}

{# Map files to potential release group mbids. #}

select local_files.filepath, map_.release_group_mbid

from {{ ref('local_files') }} as local_files
inner join {{ ref('messybrainz_name_map') }} as map_ using (recording_md5)
where map_.release_group_mbid is not null

union distinct

select filepath, release_group_mbid
from {{ ref('local_files') }}
where release_group_mbid is not null
