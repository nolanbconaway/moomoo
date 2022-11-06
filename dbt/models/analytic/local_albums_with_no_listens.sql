{{ config(materialized='view') }}

with local_albums as (
  select album_md5, min(file_created_at) as file_created_at
  from {{ ref('lastfm_local_files_flat') }}
  where album_md5 is not null
  group by 1
)

, albums_with_listens as (
  select distinct album_md5
  from {{ ref('lastfm_listens_flat') }} 
  where username = '{{ var("lastfm_username") }}'
)

select
  local_albums.album_md5
  , lastfm_albums.album_name
  , lastfm_artists.artist_name
  , local_albums.file_created_at as first_file_created_at

from local_albums
inner join {{ ref('lastfm_albums' )}} on  lastfm_albums.album_md5 = local_albums.album_md5
inner join {{ ref('lastfm_artists' )}} on lastfm_artists.artist_md5 = lastfm_albums.artist_md5
left join albums_with_listens on albums_with_listens.album_md5 = local_albums.album_md5

where albums_with_listens.album_md5 is null

order by local_albums.file_created_at desc
