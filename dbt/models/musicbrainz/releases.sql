{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['release_mbid'], 'unique': True},
      {'columns': ['release_title']},
      {'columns': ['release_group_mbid']},
      {'columns': ['artist_credit_phrase']},
    ]
  )
}}

select
  r.gid as release_mbid
  , r.name as release_title
  , rg.gid as release_group_mbid
  , frd.year as release_year
  , ac.name as artist_credit_phrase
  , r.last_updated

from {{ source('musicbrainz', 'release') }} as r
inner join {{ source('musicbrainz', 'release_group') }} as rg on r.release_group = rg.id
inner join {{ source('musicbrainz', 'artist_credit') }} as ac on r.artist_credit = ac.id
left join {{ source('musicbrainz', 'release_first_release_date') }} as frd on r.id = frd.release
