{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['artist_mbid'], 'unique': True},
      {'columns': ['artist_name']},
    ]
  )
}}

with artist_releases as (
  select
    acn.artist
    , count(distinct r.id) as release_count
    , min(frd.year) as earliest_release_year
    , max(frd.year) as latest_release_year
  from {{ source('musicbrainz', 'artist_credit_name') }} as acn
  inner join {{ source('musicbrainz', 'release') }} as r on acn.artist_credit = r.artist_credit
  left join {{ source('musicbrainz', 'release_first_release_date') }} as frd on r.id = frd.release
  group by acn.artist
)

select
  a.gid as artist_mbid
  , a.name as artist_name
  , a.comment as disambiguation
  , coalesce(ar.release_count, 0) as release_count
  , ar.earliest_release_year
  , ar.latest_release_year
  , case
    when ar.earliest_release_year is null then null
    else ar.earliest_release_year::text || '-' || ar.latest_release_year::text
  end as active_years
  , a.last_updated

from {{ source('musicbrainz', 'artist') }} as a
left join artist_releases as ar on a.id = ar.artist
