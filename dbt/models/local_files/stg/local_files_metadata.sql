{{ config(materialized='table', indexes=[{'columns': ['filepath'], 'unique': True}]) }}

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

with t as (
  select
    "filepath"
    , "recording_md5"
    , least("file_created_at", "file_modified_at") as "file_created_at"
    , {{ json_get('json_data', ['title']) }}::varchar as "track_name"
    , {{ json_get('json_data', ['album']) }}::varchar as "album_name"
    , {{ json_get('json_data', ['artist']) }}::varchar as "artist_name"
    , {{ json_get('json_data', ['album_artist']) }}::varchar as "album_artist_name"
    , {{ json_get('json_data', ['date']) }}::varchar as "track_date"
    , {{ json_get('json_data', ['length']) }}::real as "track_length_seconds"
    , {{ json_get('json_data', ['musicbrainz_trackid']) }}::varchar as "recording_mbid"
    , {{ json_get('json_data', ['musicbrainz_albumid']) }}::varchar as "release_mbid"
    , {{ json_get('json_data', ['musicbrainz_releasegroupid']) }}::varchar as "release_group_mbid"
    , {{ json_get('json_data', ['musicbrainz_artistid']) }}::varchar as "artist_mbid"
    , {{ json_get('json_data', ['musicbrainz_albumartistid']) }}::varchar as "album_artist_mbid"
    , "insert_ts_utc"

  from {{ source('pyingest', 'local_music_files') }}
)


select
  filepath
  , recording_md5
  , file_created_at
  , track_name
  , album_name
  , artist_name
  , album_artist_name
  , track_date
  , track_length_seconds
  , {{ try_cast_uuid('recording_mbid') }} as recording_mbid
  , {{ try_cast_uuid('release_mbid') }} as release_mbid
  , {{ try_cast_uuid('release_group_mbid') }} as release_group_mbid
  , {{ try_cast_uuid('artist_mbid') }} as artist_mbid
  , {{ try_cast_uuid('album_artist_mbid') }} as album_artist_mbid
  , insert_ts_utc

from t
