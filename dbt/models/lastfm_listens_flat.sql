
{{ config(
    indexes=[
      {'columns': ['listen_md5'], 'unique': True},
      {'columns': ['username']},
      {'columns': ['listen_at_ts_utc']},
      {'columns': ['track_md5']},
      {'columns': ['album_md5']},
      {'columns': ['artist_md5']},
    ]
)}}

{#
    Unpack JSON to rows. Most other models will be aggregates on this.

    Sample payload:

        {
            "artist": {
                "url": "...",
                "name": "Chuck Person",
                "image": [{ "size": "small", "#text": "..." }, ... ],
                "mbid": ""
            },
            "date": {"uts": "1654371703", "#text": "04 Jun 2022, 19:41"},
            "mbid": "",
            "name": "...",
            "image": [{ "size": "small", "#text": "..." }, ... ],
            "url": "...",
            "streamable": "0",
            "album": { "mbid": "...", #text": "..." },
            "loved": "0"
        }
#}

with t as (
    select
        "listen_md5"
        , "username"::varchar as "username"
        , "listen_at_ts_utc"
        , "insert_ts_utc"
        , nullif(trim(json_data ->> 'name'), '')::varchar as "track_name"
        , nullif(json_data ->> 'url', '')::varchar as "track_url"
        , nullif(json_data ->> 'mbid', '')::varchar as "track_mbid"
        , nullif(json_data ->> 'loved', '')::int  as "track_loved"

        , nullif(trim(json_data -> 'album' ->> '#text'), '')::varchar as "album_name"
        , nullif(json_data -> 'album' ->> 'mbid', '')::varchar as "album_mbid"

        , nullif(trim(json_data -> 'artist' ->> 'name'), '')::varchar as "artist_name"
        , nullif(json_data -> 'artist' ->> 'url', '')::varchar as "artist_url"
        , nullif(json_data -> 'artist' ->> 'mbid', '')::varchar as "artist_mbid"

    from {{ source('pyingest', 'lastfm_recent_tracks_json') }}
)


select
    listen_md5
    , username
    , listen_at_ts_utc
    , listen_at_ts_utc at time zone 'America/New_York' as listen_at_ts_nyc

    , case
        when track_name is not null and artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(track_name)', 'lower(artist_name)']) }}::varchar
        end as track_md5
    , case
        when album_name is not null and artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(album_name)', 'lower(artist_name)']) }}::varchar
        end as album_md5
    , case
        when artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(artist_name)']) }}::varchar
        end as artist_md5

    , track_name
    , track_url
    , track_mbid
    , case when track_loved = 1 then true when track_loved = 0 then false end as track_loved_as_of_insert
    , album_name
    , album_mbid
    , artist_name
    , artist_url
    , artist_mbid
    , insert_ts_utc as _insert_ts_utc

from t
