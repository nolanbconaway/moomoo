{{ config(
    indexes=[
      {'columns': ['artist_md5'], 'unique': True},
    ]
)}}

with t as (
    select
        *
        , row_number() over (partition by artist_md5 order by listen_at_ts_utc) as rownum_ 

    from {{ ref('lastfm_listens_flat') }}
    where artist_md5 is not null
)

select
    artist_md5
    , artist_name
    , artist_url
    , artist_mbid

from t
where rownum_ = 1
