{{ config(
    indexes=[
      {'columns': ['track_md5'], 'unique': True},
      {'columns': ['album_md5']},
      {'columns': ['artist_md5']},
    ]
)}}

with unioned as (
    select 
        track_md5
        , album_md5
        , artist_md5
        , track_name
        , track_url
        , track_mbid
        , 0 as is_love
        , listen_at_ts_utc as at_ts
    
    from {{ ref('lastfm_listens_flat') }}
    where track_md5 is not null
    
    union all

    select 
        track_md5
        , null as album_md5
        , artist_md5
        , track_name
        , track_url
        , track_mbid
        , 1 as is_love
        , loved_at_ts_utc as at_ts
    
    from {{ ref('lastfm_loves_flat') }}
    where track_md5 is not null

)

, ordered as (
    select
        *
        , row_number() over (partition by track_md5 order by is_love, at_ts) as rownum_ 

    from unioned
)


select track_md5, album_md5, artist_md5, track_name, track_url, track_mbid
from ordered
where rownum_ = 1
