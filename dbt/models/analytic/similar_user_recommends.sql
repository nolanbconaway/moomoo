with similars as (
  select
    from_username
    , time_range
    , entity
    , mbid
    , sum(user_similarity * log(listen_count)) as score

  from {{ ref('similar_user_activity_flat') }}

  group by 1, 2, 3, 4
)


, selfs as (
  select distinct
    username, 'artist' as entity, artist_mbid.value::uuid as mbid

  from {{ ref('listens_flat') }}
    , jsonb_array_elements_text(listens_flat.artist_mbids) as artist_mbid

  where artist_mbids is not null
    and jsonb_array_length(artist_mbids) > 0

  union all

  select distinct
    username, 'release' as entity, release_mbid as mbid
  from {{ ref('listens_flat') }}
  where release_mbid is not null

  union all

  select distinct
    username, 'recording' as entity, recording_mbid as mbid
  from {{ ref('listens_flat') }}
  where recording_mbid is not null

)

select
  similars.from_username
  , similars.time_range
  , similars.entity
  , similars.mbid
  , similars.score

from similars
left join selfs
  on selfs.username = similars.from_username
  and selfs.entity = similars.entity
  and selfs.mbid = similars.mbid

where selfs.username is null

order by 1, 2, 3, 5 desc