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
    , count(*) as release_count
  from {{ source('musicbrainz', 'release') }}
  group by release_group
),

first_release_dates as (
  select r.release_group, min(frd.year) as release_group_year

  from {{ source('musicbrainz', 'release') }} as r
  inner join {{ source('musicbrainz', 'release_first_release_date') }} as frd on frd.release = r.id
  where frd.year is not null
  group by r.release_group
)

select
  rg.gid as release_group_mbid
  , rg.name as release_group_title
  , rc.release_count
  , fd.release_group_year as release_group_year
  , pt.name as release_group_type
  , pt.name as release_group_primary_type
  , ac.name as artist_credit_phrase
  , rg.last_updated as last_updated

from {{ source('musicbrainz', 'release_group') }} as rg
left join {{ source('musicbrainz', 'release_group_primary_type') }} as pt on pt.id = rg.type
left join {{ source('musicbrainz', 'artist_credit') }} as ac on ac.id = rg.artist_credit
left join release_counts as rc on rc.release_group = rg.id
left join first_release_dates as fd on fd.release_group = rg.id