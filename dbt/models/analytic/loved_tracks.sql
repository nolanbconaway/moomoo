{{ config(materialized='view') }}

with plays as (
    select
        loves.love_md5
        , max(loves.track_md5) as track_md5
        , max(loves.username) as username
        , count(distinct listens.listen_md5) as play_count
        , count(distinct case when listens.listen_at_ts_utc > loves.loved_at_ts_utc then listens.listen_md5 end) as play_count_after_love
        , count(distinct case when listens.listen_at_ts_utc <= loves.loved_at_ts_utc then listens.listen_md5 end) as play_count_before_love
        , count(distinct case when listens.listen_at_ts_utc >= current_timestamp - interval '7 day' then listens.listen_md5 end) as play_count_7day
        , count(distinct case when listens.listen_at_ts_utc >= current_timestamp - interval '30 day' then listens.listen_md5 end) as play_count_30day
        , min(listens.listen_at_ts_nyc) as first_play_at_ts_nyc
        , max(listens.listen_at_ts_nyc) as last_play_at_ts_nyc

    from {{ ref('lastfm_loves_flat') }} as loves
    left join {{ ref('lastfm_listens_flat') }} as listens
        on loves.track_md5 = listens.track_md5
        and loves.username = listens.username
    
    group by 1
)

select
  plays.love_md5
  , plays.track_md5
  , plays.username
  , lastfm_tracks.track_name
  , lastfm_artists.artist_name
  , lastfm_albums.album_name
  , plays.play_count
  , plays.play_count_after_love
  , plays.play_count_before_love
  , plays.play_count_7day
  , plays.play_count_30day
  , plays.first_play_at_ts_nyc
  , plays.last_play_at_ts_nyc

from plays
left join {{ ref('lastfm_tracks') }}
  on lastfm_tracks.track_md5 = plays.track_md5
left join {{ ref('lastfm_albums' )}}
  on  lastfm_albums.album_md5 = lastfm_tracks.album_md5
left  join {{ ref('lastfm_artists' )}}
  on lastfm_artists.artist_md5 = lastfm_tracks.artist_md5

order by username, plays.first_play_at_ts_nyc desc