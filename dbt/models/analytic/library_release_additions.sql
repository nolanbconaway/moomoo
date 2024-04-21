{{ config(materialized='view') }}

{#
    A model with releases which may be good purchasing targets. That is, releases
    which similar users like but which are not in the library already.
#}

with similar_users as (
  select
    from_username as username
    , time_range
    , mbid as release_mbid
    , sum(user_similarity * log(listen_count)) as score

  from {{ ref('similar_user_activity') }}

  where entity = 'release'

  group by 1, 2, 3
)


, known_releases as (
  select distinct releases.release_mbid
  from {{ ref('map__file_release_group') }}
  inner join {{ ref('releases') }} as releases using (release_group_mbid)
)

, scores as (
  select
    similar_users.username
    , similar_users.time_range
    , similar_users.release_mbid
    , similar_users.score as similarity
    , row_number() over (
      partition by similar_users.username, similar_users.time_range
      order by similar_users.score desc
    ) as rank

  from similar_users
  left join known_releases using (release_mbid)

  where known_releases.release_mbid is null
)

select
  scores.username
  , scores.time_range
  , scores.rank
  , scores.release_mbid
  , releases.artist_credit_phrase
  , releases.release_title

  , scores.similarity

from scores
inner join {{ ref('releases') }} as releases using (release_mbid)

where scores.rank <= 200
order by scores.username, scores.time_range, scores.rank
