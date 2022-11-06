
{{ config(
    indexes=[
      {'columns': ['filepath'], 'unique': True},
      {'columns': ['track_md5']},
      {'columns': ['album_md5']},
      {'columns': ['artist_md5']},
    ]
)}}

{# 
    Unpack JSON to rows.

    ATTRIBUTES: Dict[str, List[str]] = dict(
        album=["album"],
        title=["title"],
        artist=["artist"],
        tracknumber=["tracknumber"],
        discnumber=["discnumber"],
        genre=["genre"],
        date=["date", "originalyear", "year", "origyear"],
        album_artist=["albumartist", "album artist"],
        musicbrainz_trackid=["musicbrainz_trackid"],
        musicbrainz_artistid=["musicbrainz_artistid"],
        musicbrainz_albumid=["musicbrainz_albumid"],
        musicbrainz_albumartistid=["musicbrainz_albumartistid"],
        musicbrainz_discid=["musicbrainz_discid"],
        musicbrainz_albumstatus=["musicbrainz_albumstatus"],
        musicbrainz_albumtype=["musicbrainz_albumtype"],
        musicbrainz_releasetrackid=["musicbrainz_releasetrackid"],
        musicbrainz_releasegroupid=["musicbrainz_releasegroupid"],
    )
#}

with extracted as (
    select
        "filepath"
        , least("file_created_at", "file_modified_at") as "file_created_at"
        , nullif(trim(json_data ->> 'title'), '')::varchar as "track_name"
        , nullif(trim(json_data ->> 'album'), '')::varchar as "album_name"
        , nullif(trim(json_data ->> 'artist'), '')::varchar as "artist_name"
        , nullif(trim(json_data ->> 'album_artist'), '')::varchar as "album_artist_name"
        , nullif(trim(json_data ->> 'date'), '')::varchar as "track_date"
        , nullif(trim(json_data ->> 'length'), '')::real as "track_length_seconds"
        , nullif(trim(json_data ->> 'musicbrainz_trackid'), '')::varchar as "track_mbid"
        , nullif(trim(json_data ->> 'musicbrainz_artistid'), '')::varchar as "artist_mbid"
        , "insert_ts_utc"

    from {{ source('pyingest', 'lastfm_local_files') }}
)

, processed as (
    select
        *
        , case
            when substring("track_date" from 1 for 4) ~ '^\d+(\.\d+)?$'
                then substring("track_date" from 1 for 4)::int
            end as "track_year"
        , coalesce(album_artist_name, artist_name ) as "derived_artist_name"
    from extracted
)

select
    "filepath"
    , case 
        when track_name is not null and derived_artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(track_name)', 'lower(derived_artist_name)']) }}::varchar
        end as track_md5
    , case 
        when album_name is not null and derived_artist_name is not null then 
        {{ dbt_utils.surrogate_key(['lower(album_name)', 'lower(derived_artist_name)']) }}::varchar
        end as album_md5
    , case 
        when derived_artist_name is not null then
        {{ dbt_utils.surrogate_key(['lower(derived_artist_name)']) }}::varchar
        end as artist_md5

    , file_created_at
    , track_name
    , album_name
    , artist_name
    , album_artist_name
    , derived_artist_name
    , track_date
    , track_year
    , track_length_seconds
    , track_mbid
    , artist_mbid
    , insert_ts_utc

from processed
