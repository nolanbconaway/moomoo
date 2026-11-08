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

select
  rg.gid as release_group_mbid
  , rg.name as release_group_title
  , rgm.release_count
  , rgm.first_release_date_year as release_group_year
  , case
    when rgm.first_release_date_year is null then null
    else make_date(
        rgm.first_release_date_year
        , coalesce(rgm.first_release_date_month, 1)
        , coalesce(rgm.first_release_date_day, 1)
      )
  end as first_release_date
  , pt.name as release_group_primary_type
  , ac.name as artist_credit_phrase
  , rg.last_updated

from {{ source('musicbrainz', 'release_group') }} as rg
inner join {{ source('musicbrainz', 'release_group_meta') }} as rgm on rg.id = rgm.id
inner join {{ source('musicbrainz', 'artist_credit') }} as ac on rg.artist_credit = ac.id
left join {{ source('musicbrainz', 'release_group_primary_type') }} as pt on rg.type = pt.id
