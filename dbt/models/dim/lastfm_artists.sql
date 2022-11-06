{{ config(
    indexes=[
      {'columns': ['artist_md5'], 'unique': True},
    ]
)}}

with unioned as (
    select
        artist_md5
        , max(artist_name) as artist_name
        , max(artist_url) as artist_url
        , max(artist_mbid) as artist_mbid
        , 0 as priority
        , min(listen_at_ts_utc) as at_ts
        , 'lastfm-listens' as source

    from {{ ref('lastfm_listens_flat') }}
    where artist_md5 is not null
    group by 1

    union all

    select
        artist_md5
        , max(artist_name) as artist_name
        , max(artist_url) as artist_url
        , max(artist_mbid) as artist_mbid
        , 1 as priority
        , min(loved_at_ts_utc) as at_ts
        , 'lastfm-loves' as source

    from {{ ref('lastfm_loves_flat') }}
    where artist_md5 is not null
    group by 1


    union all

    select
        artist_md5
        , max(derived_artist_name) as artist_name
        , null as artist_url
        , max(artist_mbid) as artist_mbid
        , 2 as priority
        , min(file_created_at) as at_ts
        , 'local-files' as source

    from {{ ref('lastfm_local_files_flat') }}
    where artist_md5 is not null
    group by 1
)

, ordered as (
    select
        *
        , row_number() over (partition by artist_md5 order by priority, at_ts) as rownum_

    from unioned
)


select artist_md5, artist_name, artist_url, artist_mbid, source::varchar as source
from ordered
where rownum_ = 1
