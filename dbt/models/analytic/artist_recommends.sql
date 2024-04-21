{{ config(materialized='view') }}

{# Artists which are popular with similar users #}

with similar_users as (
  select
    from_username as username
    , mbid as artist_mbid
    , sum(user_similarity * log(listen_count)) as score

  from {{ ref("similar_user_activity") }}

  where entity = 'artist'
    and time_range = 'all_time'

  group by 1, 2
)

, lb_top as (
  select artist_mbid
  from {{ ref("listenbrainz_artist_stats") }}
  order by total_listen_count desc
  limit 100
)

, avg_listen_count as (
  select avg(total_listen_count) as avg_listen_count
  from {{ ref("listenbrainz_artist_stats") }}
)

, novelty as (
  select
    stats_.artist_mbid
    , log(avg_.avg_listen_count + 1) / log(stats_.total_listen_count + 1) as score
  from {{ ref("listenbrainz_artist_stats") }} as stats_
  cross join avg_listen_count as avg_

)

, known_artists as (
  select username, artist_mbid
  from {{ ref("artist_listen_counts") }}
  where lifetime_listen_count > 5
)

, scores as (
  select
    username
    , artist_mbid
    , similar_users.score as similarity
    , novelty.score as novelty
    , row_number() over (
      partition by username
      order by novelty.score * similar_users.score desc
    ) as rank

  from similar_users
  inner join novelty using (artist_mbid)
  left join lb_top using (artist_mbid)
  left join known_artists using (artist_mbid, username)

  where known_artists.artist_mbid is null
    and lb_top.artist_mbid is null
)

select
  scores.username
  , scores.rank
  , scores.artist_mbid
  , artists.artist_name
  , scores.similarity
  , scores.novelty

from scores
inner join {{ ref("artists") }} as artists using (artist_mbid)

where scores.rank <= 200
order by scores.username, scores.rank
