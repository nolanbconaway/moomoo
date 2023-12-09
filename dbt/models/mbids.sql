{{ config(materialized='view') }}

{# Mega model with many deps, containing all known mbids. #}

with release_mbids as (
  select distinct release_mbid as mbid
  from {{ ref('listens') }}
  where release_mbid is not null

  union distinct

  select distinct release_mbid as mbid
  from {{ ref('local_files') }}
  where release_mbid is not null

  union distinct

  select distinct release_mbid as mbid
  from {{ ref('messybrainz_name_map') }}
  where release_mbid is not null

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity') }}
  where entity = 'release'
)

, release_group_mbids as (
  select distinct release_group_mbid as mbid
  from {{ ref('local_files') }}
  where release_group_mbid is not null

  union distinct

  select distinct release_group_mbid as mbid
  from {{ ref('messybrainz_name_map') }}
  where release_group_mbid is not null

  union distinct

  {# NOTE: weird here but we only know the release group for listen data AFTER querying musicbrainz. #}
  select distinct release_group_mbid as mbid
  from {{ ref('releases') }}
  where release_group_mbid is not null
)

, recording_mbids as (
  select distinct recording_mbid as mbid
  from {{ ref('listens') }}
  where recording_mbid is not null

  union distinct

  select distinct recording_mbid as mbid
  from {{ ref('messybrainz_name_map') }}
  where recording_mbid is not null

  union distinct

  select distinct recording_mbid as mbid
  from {{ ref('local_files') }}
  where recording_mbid is not null

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity') }}
  where entity = 'recording'
)

, artist_mbids as (
  select distinct artist_mbid.value::uuid as mbid
  from {{ ref('listens') }} as listens
  , jsonb_array_elements_text(listens.artist_mbids) as artist_mbid
  where listens.artist_mbids is not null
    and jsonb_array_length(listens.artist_mbids) > 0

  union distinct

  select distinct artist_mbid as mbid
  from {{ ref('local_files') }}
  where artist_mbid is not null

  union distinct

  select distinct album_artist_mbid as mbid
  from {{ ref('local_files') }}
  where album_artist_mbid is not null

  union distinct

  select distinct artist_mbid.value::uuid as mbid
  from {{ ref('messybrainz_name_map') }} as _map
  , jsonb_array_elements_text(_map.artist_mbids) as artist_mbid
  where _map.artist_mbids is not null
    and jsonb_array_length(_map.artist_mbids) > 0

  union distinct

  select distinct mbid
  from {{ ref('similar_user_activity') }}
  where entity = 'artist'
)

select
  mbid
  , 'release'::varchar as entity
from release_mbids

union all

select
  mbid
  , 'release-group'::varchar as entity
from release_group_mbids

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
