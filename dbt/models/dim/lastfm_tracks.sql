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
        , max(album_md5) as album_md5
        , max(artist_md5) as artist_md5
        , max(track_name) as track_name
        , max(track_url) as track_url
        , max(track_mbid) as track_mbid
        , 0 as priority
        , min(listen_at_ts_utc) as at_ts
        , 'lastfm-listens' as source

    from {{ ref('lastfm_listens_flat') }}
    where track_md5 is not null
    group by 1

    union all

    select
        track_md5
        , null as album_md5
        , max(artist_md5) as artist_md5
        , max(track_name) as track_name
        , max(track_url) as track_url
        , max(track_mbid) as track_mbid
        , 1 as priority
        , min(loved_at_ts_utc) as at_ts
        , 'lastfm-loves' as source

    from {{ ref('lastfm_loves_flat') }}
    where track_md5 is not null
    group by 1

    union all

    select
        artist_md5
        , max(album_md5) as album_md5
        , max(artist_md5) as artist_md5
        , max(track_name) as track_name
        , null as track_url
        , max(track_mbid) as track_mbid
        , 2 as priority
        , min(file_created_at) as at_ts
        , 'local-files' as source

    from {{ ref('lastfm_local_files_flat') }}
    where track_md5 is not null
    group by 1
)

, ordered as (
    select
        *
        , row_number() over (partition by track_md5 order by priority, at_ts) as rownum_

    from unioned
)


select
    track_md5
    , album_md5
    , artist_md5
    , track_name
    , track_url
    , track_mbid
    , source::varchar as source

from ordered
where rownum_ = 1
