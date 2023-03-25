{{ config(materialized='view') }}

{# Top releases listened by year releases. #}

with ordered as (
  select
    listens.username
    , release.release_mbid
    , count(1) as count_listens
    , row_number() over (
      partition by listens.username, max(release.release_year)
      order by count(1) desc
    ) as rank

  from {{ ref('listens_flat') }} as listens
  inner join {{ ref('dim_release') }} as release
  on listens.release_mbid = release.release_mbid
    and release.release_year is not null

  group by 1, 2
)


select
  ordered.username
  , release.release_mbid
  , release.artist_credit_phrase
  , release.release_title
  , release.release_year
  , ordered.count_listens
  , ordered.rank

from ordered

inner join {{ ref('dim_release') }} as release
  on ordered.release_mbid = release.release_mbid

where ordered.rank <= 5

order by ordered.username, release.release_year desc, ordered.rank