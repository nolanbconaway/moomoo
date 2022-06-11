{{ config(materialized='view') }}

with loves as (
    select distinct track_md5, username
    from {{ ref('lastfm_listens_flat') }}
    where track_loved
)

, plays as (
    select
        loves.track_md5
        , loves.username
        , count(*) as play_count
        , count(case when listen_at_ts_utc >= current_timestamp - interval '7 day' then 1 end) as play_count_7day
        , count(case when listen_at_ts_utc >= current_timestamp - interval '30 day' then 1 end) as play_count_30day
        , min(listen_at_ts_nyc) as first_play_at_ts_nyc
        , max(listen_at_ts_nyc) as last_play_at_ts_nyc

    from loves
    join {{ ref('lastfm_listens_flat') }}
        on loves.track_md5 = lastfm_listens_flat.track_md5
        and loves.username = lastfm_listens_flat.username
    group by loves.track_md5, loves.username
)

select
  plays.track_md5
  , plays.username
  , lastfm_tracks.track_name
  , lastfm_artists.artist_name
  , lastfm_albums.album_name
  , plays.play_count
  , plays.play_count_7day
  , plays.play_count_30day
  , plays.first_play_at_ts_nyc
  , plays.last_play_at_ts_nyc

from plays
join {{ ref('lastfm_tracks') }}
  on lastfm_tracks.track_md5 = plays.track_md5
join {{ ref('lastfm_albums' )}}
  on  lastfm_albums.album_md5 = lastfm_tracks.album_md5
join {{ ref('lastfm_artists' )}}
  on lastfm_artists.artist_md5 = lastfm_tracks.artist_md5

order by username, plays.first_play_at_ts_nyc desc