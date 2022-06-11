{{ config(
    indexes=[
      {'columns': ['username'], 'unique': True},
    ]
)}}

select
    username
    , count(distinct case when track_loved then track_md5 end) as loved_count
    , count(1) as play_count
    , count(case when listen_at_ts_utc >= current_timestamp - interval '7 day' then 1 end) as play_count_7day
    , count(case when listen_at_ts_utc >= current_timestamp - interval '30 day' then 1 end) as play_count_30day
    , min(listen_at_ts_nyc) as first_play_at_ts_nyc
    , max(listen_at_ts_nyc) as last_play_at_ts_nyc


from  {{ ref('lastfm_listens_flat') }}
group by 1
