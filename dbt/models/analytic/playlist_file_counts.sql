{{ config(materialized='view') }}

with agg as (
  select
    collections.username
    , track.value ->> 'filepath' as filepath
    , count(distinct playlist.playlist_id) as playlist_count
    , array_agg(distinct collections.collection_name) as collection_names

  from {{ ref('moomoo_playlist_collection_items') }} as playlist
  inner join {{ ref('moomoo_playlist_collections') }} as collections using (collection_id)
  , jsonb_array_elements(playlist.playlist) as track

  where collections.collection_name not in ('loved-tracks', 'revisit-tracks')

  group by 1, 2
  having count(distinct playlist.playlist_id) > 1
)

select
  agg.username
  , agg.filepath
  , local_files.track_name
  , local_files.album_name
  , local_files.artist_name
  , local_files.album_artist_name
  , agg.playlist_count
  , agg.collection_names

from agg
inner join {{ ref('local_files') }} as local_files using (filepath)

order by agg.username asc, agg.playlist_count desc
