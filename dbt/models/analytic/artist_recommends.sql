{{
    config(
      materialized='table',
      indexes=[
          {'columns': ['artist_mbid'], 'unique': True},
        ]
    )
}}

{# artists which are similar to highly listened artists, but not in the user's library #}

with top_n as (
  select
    artist_mbid
    , ln(lifetime_listen_count) as log_listens
  from {{ ref('artist_listen_counts') }}
  where lifetime_listen_count > 50
  order by lifetime_listen_count desc
  limit 500
)

, in_library as (
  select artist_mbid
  from {{ ref('map__file_artist') }}
  group by 1
  having count(1) > 6
)

, scores as (
  select
    collab.artist_mbid_a
    , collab.artist_mbid_b
    , exp(collab.score_value - 0.35) as score_value
    , top_n.log_listens

  from {{ ref('listenbrainz_collaborative_filtering_scores') }} as collab
  inner join top_n on collab.artist_mbid_a = top_n.artist_mbid
  left join in_library on collab.artist_mbid_b = in_library.artist_mbid

  -- only suggest artists not already in the library
  where in_library.artist_mbid is null
)

, max_score_artist as (
  -- top score from an artist in the library
  select distinct on (scores.artist_mbid_b)
    scores.artist_mbid_b
    , scores.artist_mbid_a
    , scores.score_value
  from scores
  inner join in_library
    on scores.artist_mbid_a = in_library.artist_mbid
  order by scores.artist_mbid_b asc, scores.score_value desc
)

, agg_scores as (
  select
    artist_mbid_b
    , sum(score_value * log_listens) as total_score
    , max(score_value) as max_score

  from scores
  group by 1
)

select
  agg_scores.artist_mbid_b as artist_mbid
  , artist_b.artist_name
  , artist_a.artist_name as most_similar_artist_name
  , artist_b.tags_string
  , artist_b.active_years
  , agg_scores.total_score
  , agg_scores.max_score
  , max_score_artist.score_value as max_similar_artist_score

from agg_scores
inner join max_score_artist using (artist_mbid_b)
left join {{ ref('artists') }} as artist_b on agg_scores.artist_mbid_b = artist_b.artist_mbid
left join {{ ref('artists') }} as artist_a on max_score_artist.artist_mbid_a = artist_a.artist_mbid

order by agg_scores.max_score desc
