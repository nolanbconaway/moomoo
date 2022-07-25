
{{ config(
    indexes=[
      {'columns': ['love_md5'], 'unique': True},
      {'columns': ['username']},
      {'columns': ['loved_at_ts_utc']},
      {'columns': ['track_md5']},
      {'columns': ['artist_md5']},
    ]
)}}

{# 
    Unpack JSON to rows.

    Sample Payload:

        {
            "artist": {
                "url": "https://www.last.fm/music/Yuji+Toriyama",
                "name": "Yuji Toriyama",
                "mbid": ""
            },
            "date": {"uts": "1657288699", "#text": "08 Jul 2022, 13:58"},
            "mbid": "",
            "url": "https://www.last.fm/music/Yuji+Toriyama/_/Korean+Dress+(Part+2)",
            "name": "Korean Dress (Part 2)",
            "image": [
                {"size": "small", "#text": "..."},
                {"size": "medium", "#text": "..."},
                {"size": "large", "#text": "..."},
                {"size": "extralarge", "#text": "..."}
            ],
            "streamable": {"fulltrack": "0", "#text": "0"}
        }
#}

with t as (
    select
        "love_md5"
        , "username"
        , "loved_at_ts_utc"
        , "insert_ts_utc"
        
        , nullif(trim(json_data ->> 'name'), '') as "track_name"
        , nullif(json_data ->> 'url', '') as "track_url"
        , nullif(json_data ->> 'mbid', '') as "track_mbid"

        , nullif(trim(json_data -> 'artist' ->> 'name'), '') as "artist_name"
        , nullif(json_data -> 'artist' ->> 'url', '') as "artist_url"
        , nullif(json_data -> 'artist' ->> 'mbid', '') as "artist_mbid"

    from {{ source('pyingest', 'lastfm_loved_tracks_json') }}
)

select
    love_md5
    , username
    , loved_at_ts_utc
    , loved_at_ts_utc at time zone 'America/New_York' as loved_at_ts_nyc
    
    , case 
        when track_name is not null and artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(track_name)', 'lower(artist_name)']) }} 
        end as track_md5
    , case 
        when artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(artist_name)']) }} 
        end as artist_md5

    , track_name
    , track_url
    , track_mbid
    , artist_name
    , artist_url
    , artist_mbid
    , insert_ts_utc as _insert_ts_utc

from t
