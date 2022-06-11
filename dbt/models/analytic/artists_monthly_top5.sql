{{ config(materialized='view') }}

with grouped as (
    select 
        username
        , artist_md5
        , date_trunc('month', listen_at_ts_nyc)::date as month_start
        , count(1) as play_count

    from {{ ref('lastfm_listens_flat') }}
    
    where listen_at_ts_nyc < date_trunc('month', current_timestamp at time zone 'America/New_York')
      and listen_at_ts_nyc >= '2022-01-01 00:00:00'::timestamp
      and artist_md5 is not null
    
    group by 1, 2, 3
)

, ranked as (
    select 
        *
        , row_number() over (partition by username, month_start order by play_count desc) as artist_rank
    from grouped
)

, annotated as (
    select 
        month_start
        , username
        , artist_rank
        , concat(lastfm_artists.artist_name, ' (', play_count, ')') as annotation

    from ranked
    join {{ ref('lastfm_artists') }} on ranked.artist_md5 = lastfm_artists.artist_md5

    where artist_rank <= 5
)


select
    username
    , month_start
    , {{ 
        dbt_utils.pivot(
            column='artist_rank',
            values=[1, 2, 3, 4, 5],
            agg='min',
            prefix='rank_',
            then_value='annotation',
            else_value='null',
        )
    }}
from annotated
group by username, month_start
order by username, month_start desc