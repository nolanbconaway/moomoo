{{ config(
    indexes=[
      {'columns': ['artist_md5'], 'unique': True},
    ]
)}}

with unioned as (
    select 
        artist_md5
        , artist_name
        , artist_url
        , artist_mbid
        , 0 as is_love
        , listen_at_ts_utc as at_ts
    
    from {{ ref('lastfm_listens_flat') }}
    where track_md5 is not null
    
    union all

    select 
        artist_md5
        , artist_name
        , artist_url
        , artist_mbid
        , 1 as is_love
        , loved_at_ts_utc as at_ts
    
    from {{ ref('lastfm_loves_flat') }}
    where track_md5 is not null

)

, ordered as (
    select
        *
        , row_number() over (partition by artist_md5 order by is_love, at_ts) as rownum_ 

    from unioned
)


select artist_md5, artist_name, artist_url, artist_mbid
from ordered
where rownum_ = 1
