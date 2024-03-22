{# Public view of the local files data. #}
{{ config(
  materialized='table',
  indexes=[
    {'columns': ['filepath'], 'unique': True},
    {'columns': ['recording_md5']},
    {'columns': ['recording_mbid']},
    {'columns': ['release_mbid']},
    {'columns': ['release_group_mbid']},
    {'columns': ['artist_mbid']},
    {'columns': ['album_artist_mbid']},
    {'columns': ['track_name']},
    {'columns': ['album_name']},
    {'columns': ['artist_name']},
    {'columns': ['album_artist_name']},
    {'columns': ['embedding_success']},
    {'columns': ['embedding_duration_seconds']},
  ]
) }}


with metadata as (
  select
    *
    , {{ extract_year('track_date') }} as track_year
  from {{ ref('local_files_metadata') }}
)

, embeds as (
  select *
  from {{ ref('local_files_embedding') }}
)

select
  metadata.filepath
  , metadata.recording_md5
  , metadata.file_created_at
  , metadata.track_name
  , metadata.album_name
  , metadata.artist_name
  , metadata.album_artist_name
  , metadata.track_date
  , metadata.track_year
  , metadata.track_length_seconds
  , metadata.recording_mbid
  , metadata.release_mbid
  , metadata.release_group_mbid
  , metadata.artist_mbid
  , metadata.album_artist_mbid
  , metadata.insert_ts_utc

  , embeds.success as embedding_success
  , embeds.duration_seconds as embedding_duration_seconds
  , embeds.embedding
  , embeds.insert_ts_utc as embedding_insert_ts_utc

from metadata
left join embeds using (filepath)
