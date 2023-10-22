{{ config(materialized='view') }}

{# Top releases listened by year releases. #}

with ordered as (
  select
    listens.username
    , releases.release_mbid
    , count(1) as count_listens
    , row_number() over (
      partition by listens.username, max(releases.release_year)
      order by count(1) desc
    ) as rank

  from {{ ref('listens_flat') }} as listens
  inner join {{ ref('dim_release') }} as releases
    on listens.release_mbid = releases.release_mbid
      and releases.release_year is not null

  group by 1, 2
)


select
  ordered.username
  , releases.release_mbid
  , releases.artist_credit_phrase
  , releases.release_title
  , releases.release_year
  , ordered.count_listens
  , ordered.rank

from ordered

inner join {{ ref('dim_release') }} as releases
  on ordered.release_mbid = releases.release_mbid

where ordered.rank <= 5

order by --noqa: AM06
  ordered.username asc
  , releases.release_year desc
  , ordered.rank asc
