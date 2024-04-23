{{ config(materialized='view') }}

{#
    A model with releases which may be good purchasing targets. That is, releases
    which similar users like but which are not in the library already.
#}

with similar_users as (
  select
    sua.from_username as username
    , sua.time_range
    , releases.release_group_mbid
    , sum(sua.user_similarity * log(sua.listen_count)) as score

  from {{ ref('similar_user_activity') }} as sua
  inner join {{ ref('releases') }} as releases
    on sua.mbid = releases.release_mbid

  where sua.entity = 'release'

  group by 1, 2, 3
)


, known_releases as (
  select distinct release_group_mbid
  from {{ ref('map__file_release_group') }}
)

, scores as (
  select
    similar_users.username
    , similar_users.time_range
    , similar_users.release_group_mbid
    , similar_users.score as score
    , row_number() over (
      partition by similar_users.username, similar_users.time_range
      order by similar_users.score desc
    ) as rank

  from similar_users
  left join known_releases using (release_group_mbid)

  where known_releases.release_group_mbid is null
)

select
  scores.username
  , scores.time_range
  , scores.rank
  , scores.release_group_mbid
  , release_groups.artist_credit_phrase
  , release_groups.release_group_title
  , scores.score

from scores
inner join {{ ref('release_groups') }} as release_groups using (release_group_mbid)

where scores.rank <= 200
order by scores.username, scores.time_range, scores.rank
