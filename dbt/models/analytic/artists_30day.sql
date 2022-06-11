{{ config(materialized='view') }}

select
    artist_md5
    , username
    , max(artist_name) as artist_name
    , count(1) as listen_count
    , count(distinct coalesce(track_md5, '')) as track_count
    , count(distinct coalesce(album_md5, '')) as album_count
    , max(listen_at_ts_nyc) as last_listen_at_ts_nyc

from {{ ref('lastfm_listens_flat') }}
where listen_at_ts_nyc::date >= current_date - interval '30 day'
group by 1, 2
order by username, listen_count desc