{{ config(materialized='view') }}

with plays as (
  select 
    lastfm_listens_flat.album_md5
    , lastfm_listens_flat.username
    , min(lastfm_listens_flat.listen_at_ts_nyc) as first_listen_at_ts_nyc
    , max(lastfm_listens_flat.listen_at_ts_nyc) as last_listen_at_ts_nyc
    , count(1) as listen_count
    , count(distinct lastfm_listens_flat.track_md5) as track_count

  from {{ ref('lastfm_listens_flat') }} 
  where lastfm_listens_flat.album_md5 is not null
  group by 1, 2
)

select
  plays.album_md5
  , plays.username
  , lastfm_albums.album_name
  , lastfm_artists.artist_name
  , extract(
      days from plays.last_listen_at_ts_nyc - plays.first_listen_at_ts_nyc
    ) / 365 as listen_lifetime_years
  , plays.listen_count
  , plays.track_count
  , plays.listen_count / plays.track_count::float as avg_listens_per_track

from plays
join {{ ref('lastfm_albums' )}}
  on  lastfm_albums.album_md5 = plays.album_md5
join {{ ref('lastfm_artists' )}}
  on lastfm_artists.artist_md5 = lastfm_albums.artist_md5

order by username, listen_count desc