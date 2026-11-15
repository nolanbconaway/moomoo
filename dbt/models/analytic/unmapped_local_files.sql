{{
    config(
      materialized='table',
      indexes=[{'columns': ['filepath'], 'unique': True}]
    )
}}

{# a list of local files whos mbids are unmapped in the mbz database. #}
with t as (
  select local_files.filepath, 'recording' as entity
  from {{ ref('local_files') }} as local_files
  left join {{ ref('recordings') }} as recordings using (recording_mbid)
  where recordings.recording_mbid is null
    and local_files.recording_mbid is not null

  union

  select local_files.filepath, 'release' as entity
  from {{ ref('local_files') }} as local_files
  left join {{ ref('releases') }} as releases using (release_mbid)
  where releases.release_mbid is null
    and local_files.release_mbid is not null

  union

  select local_files.filepath, 'release-group' as entity
  from {{ ref('local_files') }} as local_files
  left join {{ ref('release_groups') }} as release_groups using (release_group_mbid)
  where release_groups.release_group_mbid is null
    and local_files.release_group_mbid is not null

  union distinct

  select local_files.filepath, 'artist' as entity
  from {{ ref('local_files') }} as local_files
  left join {{ ref('artists') }} as artists using (artist_mbid)
  where artists.artist_mbid is null
    and local_files.artist_mbid is not null

  union

  select local_files.filepath, 'album-artist' as entity
  from {{ ref('local_files') }} as local_files
  left join {{ ref('artists') }} as artists
    on local_files.album_artist_mbid = artists.artist_mbid
  where artists.artist_mbid is null
    and local_files.artist_mbid is not null
)

select filepath, array_agg(entity) as unmatched_entities
from t
group by filepath
