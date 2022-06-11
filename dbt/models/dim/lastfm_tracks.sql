{{ config(
    indexes=[
      {'columns': ['track_md5'], 'unique': True},
      {'columns': ['album_md5']},
      {'columns': ['artist_md5']},
    ]
)}}

with t as (
    select
        *
        , row_number() over (partition by track_md5 order by listen_at_ts_utc) as rownum_ 

    from {{ ref('lastfm_listens_flat') }}
    where track_md5 is not null
)

select
    track_md5
    , album_md5
    , artist_md5
    , track_name
    , track_url
    , track_mbid

from t
where rownum_ = 1
