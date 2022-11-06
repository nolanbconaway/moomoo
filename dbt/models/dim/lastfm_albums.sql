{{ config(
    indexes=[
      {'columns': ['album_md5'], 'unique': True},
      {'columns': ['artist_md5']},
    ]
)}}

with unioned as (
    select
        album_md5
        , max(artist_md5) as artist_md5
        , max(album_name) as album_name
        , max(album_mbid) as album_mbid
        , 0 as priority
        , min(listen_at_ts_utc) as at_ts
        , 'lastfm-listens' as source

    from {{ ref('lastfm_listens_flat') }}
    where album_md5 is not null
    group by 1

    union all

    select
        album_md5
        , max(artist_md5) as artist_md5
        , max(album_name) as album_name
        , null as album_mbid
        , 1 as priority
        , min(file_created_at) as at_ts
        , 'local-files' as source

    from {{ ref('lastfm_local_files_flat') }}
    where album_md5 is not null
    group by 1
)

, ordered as (
    select
        *
        , row_number() over (partition by album_md5 order by priority, at_ts) as rownum_

    from unioned
)

select
    album_md5
    , artist_md5
    , album_name
    , album_mbid
    , source::varchar as source

from ordered
where rownum_ = 1
