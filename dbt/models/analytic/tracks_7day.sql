{{ config(materialized='view') }}

with plays as (
  select 
    track_md5
    , username
    , count(1) as listen_count
    , max(listen_at_ts_nyc) as last_listen_at_ts_nyc
  from {{ ref('lastfm_listens_flat') }}
  where listen_at_ts_nyc::date >= current_date - interval '7 day'
  group by 1, 2
)

select
  plays.track_md5
  , plays.username
  , lastfm_tracks.track_name
  , lastfm_artists.artist_name
  , lastfm_albums.album_name
  , plays.listen_count
  , plays.last_listen_at_ts_nyc

from plays
join {{ ref('lastfm_tracks') }}
  on lastfm_tracks.track_md5 = plays.track_md5
join {{ ref('lastfm_albums' )}}
  on  lastfm_albums.album_md5 = lastfm_tracks.album_md5
join {{ ref('lastfm_artists' )}}
  on lastfm_artists.artist_md5 = lastfm_tracks.artist_md5

order by username, listen_count desc