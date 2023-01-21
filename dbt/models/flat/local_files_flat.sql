
{{ config(
  indexes=[
    {'columns': ['filepath'], 'unique': True},
  ]
) }}

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
    , {{ json_get('json_data', ['title']) }}::varchar as "track_name"
    , {{ json_get('json_data', ['album']) }}::varchar as "album_name"
    , {{ json_get('json_data', ['artist']) }}::varchar as "artist_name"
    , {{ json_get('json_data', ['album_artist']) }}::varchar as "album_artist_name"
    , {{ json_get('json_data', ['date']) }}::varchar as "track_date"
    , {{ json_get('json_data', ['length']) }}::real as "track_length_seconds"
    , {{ json_get('json_data', ['musicbrainz_trackid']) }}::varchar as "track_mbid"
    , {{ json_get('json_data', ['musicbrainz_artistid']) }}::varchar as "artist_mbid"
    , "insert_ts_utc"

  from {{ source('pyingest', 'local_music_files') }}
)

, processed as (
  select
    *
    , case
      when substring("track_date" from 1 for 4) ~ '^\d+(\.\d+)?$'
        then substring("track_date" from 1 for 4)::int
    end as "track_year"
  from extracted
)

select
  "filepath"
  , file_created_at
  , track_name
  , album_name
  , artist_name
  , album_artist_name
  , track_date
  , track_year
  , track_length_seconds
  , track_mbid
  , artist_mbid
  , insert_ts_utc

from processed