{{ config(
    materialized='table',
    indexes=[{'columns': ['filepath', 'release_mbid'], 'unique': True}]
  )
}}

{# Map files to potential release mbids. #}

select local_files.filepath, map_.release_mbid

from {{ ref('local_files') }} as local_files
inner join {{ ref('messybrainz_name_map') }} as map_ using (recording_md5)
where map_.release_mbid is not null

union distinct

select filepath, release_mbid
from {{ ref('local_files') }}
where release_mbid is not null
