{{ config(materialized='view') }}

with release_mbids as (
  select distinct release_mbid as mbid
  from {{ ref('listens_flat') }}
  where release_mbid is not null

  union distinct

  select distinct release_mbid as mbid
  from {{ ref('local_files_flat') }}
  where release_mbid is not null

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity_flat') }}
  where entity = 'release'
)

, recording_mbids as (
  select distinct recording_mbid as mbid
  from {{ ref('listens_flat') }}
  where recording_mbid is not null

  union distinct

  select distinct recording_mbid as mbid
  from {{ ref('local_files_flat') }}
  where recording_mbid is not null

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity_flat') }}
  where entity = 'recording'
)

, artist_mbids as (
  select distinct artist_mbid.value::uuid as mbid
  from {{ ref('listens_flat') }} as listens_flat
  , jsonb_array_elements_text(listens_flat.artist_mbids) as artist_mbid
  where listens_flat.artist_mbids is not null
    and jsonb_array_length(listens_flat.artist_mbids) > 0

  union distinct

  select distinct artist_mbid as mbid
  from {{ ref('local_files_flat') }}
  where artist_mbid is not null

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity_flat') }}
  where entity = 'artist'
)

select
  mbid
  , 'release'::varchar as entity
from release_mbids

union all

select
  mbid
  , 'recording'::varchar as entity
from recording_mbids

union all

select
  mbid
  , 'artist'::varchar as entity
from artist_mbids
