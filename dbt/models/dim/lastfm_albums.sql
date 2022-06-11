{{ config(
    indexes=[
      {'columns': ['album_md5'], 'unique': True},
      {'columns': ['artist_md5']},
    ]
)}}

with t as (
    select
        *
        , row_number() over (partition by album_md5 order by listen_at_ts_utc) as rownum_ 

    from {{ ref('lastfm_listens_flat') }}
    where album_md5 is not null
)

select
    album_md5
    , artist_md5
    , album_name
    , album_mbid

from t
where rownum_ = 1
