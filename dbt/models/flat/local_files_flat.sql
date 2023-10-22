{{ config(
  indexes=[
    {'columns': ['filepath'], 'unique': True},
    {'columns': ['recording_msid']},
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
    , {{ json_get('json_data', ['musicbrainz_trackid']) }}::varchar as "recording_mbid"
    , {{ json_get('json_data', ['musicbrainz_albumid']) }}::varchar as "release_mbid"
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
    , {{ recording_md5( 'track_name', 'artist_name', 'album_name') }} as recording_md5
  from extracted
)

, msid as (
  select distinct processed.filepath, listens.recording_msid
  from processed
  left join {{ ref('listens_flat') }} as listens
    on processed.recording_md5 = listens.recording_md5
)

select
  processed.filepath
  , processed.file_created_at
  , processed.track_name
  , processed.album_name
  , processed.artist_name
  , processed.album_artist_name
  , processed.track_date
  , processed.track_year
  , processed.track_length_seconds
  , {{ try_cast_uuid('processed.recording_mbid') }} as recording_mbid
  , {{ try_cast_uuid('processed.release_mbid') }} as release_mbid
  , {{ try_cast_uuid('processed.artist_mbid') }} as artist_mbid
  , processed.insert_ts_utc

  , processed.recording_md5
  , msid.recording_msid

  , embeds.success as embedding_success
  , embeds.duration_seconds as embedding_duration_seconds
  , embeds.embedding as embedding
  , embeds.insert_ts_utc as embedding_insert_ts_utc

from processed
left join msid on processed.filepath = msid.filepath
left join {{ source('pyingest', 'local_music_embeddings') }} as embeds
  on processed.filepath = embeds.filepath
