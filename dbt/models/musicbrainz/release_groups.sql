{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['release_group_mbid'], 'unique': True},
      {'columns': ['release_group_title']},
      {'columns': ['artist_credit_phrase']},
    ]
  )
}}

with release_counts as (
  select
    release_group
    , count(1) as release_count
  from {{ source('musicbrainz', 'release') }}
  group by release_group
)

, first_release_dates as (
  select
    r.release_group
    , min(frd.year) as release_group_year

  from {{ source('musicbrainz', 'release') }} as r
  inner join {{ source('musicbrainz', 'release_first_release_date') }} as frd on r.id = frd.release
  where frd.year is not null
  group by r.release_group
)

select
  rg.gid as release_group_mbid
  , rg.name as release_group_title
  , rc.release_count
  , fd.release_group_year
  , pt.name as release_group_type
  , pt.name as release_group_primary_type
  , ac.name as artist_credit_phrase
  , rg.last_updated

from {{ source('musicbrainz', 'release_group') }} as rg
left join {{ source('musicbrainz', 'release_group_primary_type') }} as pt on rg.type = pt.id
left join {{ source('musicbrainz', 'artist_credit') }} as ac on rg.artist_credit = ac.id
left join release_counts as rc on rg.id = rc.release_group
left join first_release_dates as fd on rg.id = fd.release_group
