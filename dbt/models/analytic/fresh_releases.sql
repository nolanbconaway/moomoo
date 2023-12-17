{{ config(materialized='view') }}

{# Albums from artists without a lot of listens, ranked by similar user score. #}

with similar_users as (
  select
    from_username as username
    , time_range
    , entity
    , mbid
    , sum(user_similarity * log(listen_count)) as score

  from {{ ref('similar_user_activity') }}

  group by 1, 2, 3, 4
)

, skip_releases as (
  {# skip releases containing an artist with > 5 listens #}
  select alc.username, r.release_mbid

  from {{ ref('releases') }} as r
  inner join {{ ref('map__release_group_artist') }} as ra using (release_group_mbid)
  inner join {{ ref('artist_listen_counts') }} as alc
    on ra.artist_mbid = alc.artist_mbid

  group by 1, 2
  having max(alc.lifetime_listen_count) > 5
)

, release_scores as (
  select
    s.username
    , s.time_range
    , concat(r.release_title, ' - ', r.artist_credit_phrase) as description_text
    , row_number() over (partition by s.username, s.time_range order by s.score desc) as rank
  from similar_users as s
  inner join {{ ref('releases') }} as r
    on r.release_mbid = s.mbid
      and s.entity = 'release'
  left join skip_releases
    on skip_releases.release_mbid = r.release_mbid
      and skip_releases.username = s.username

  where skip_releases.release_mbid is null
)

select
  username
  , rank
  , max(case when time_range = 'all_time' then description_text end) as "all_time"
  , max(case when time_range = 'year' then description_text end) as "year"
  , max(case when time_range = 'month' then description_text end) as "month"

from release_scores

where rank <= 50

group by 1, 2
order by 1, 2
