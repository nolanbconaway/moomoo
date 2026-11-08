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
  , r.last_updated as last_updated

from {{ source('musicbrainz', 'release') }} as r
inner join {{ source('musicbrainz', 'release_group') }} as rg on rg.id = r.release_group
inner join {{ source('musicbrainz', 'artist_credit') }} as ac on ac.id = r.artist_credit
left join {{ source('musicbrainz', 'release_first_release_date') }} as frd on frd.release = r.id
