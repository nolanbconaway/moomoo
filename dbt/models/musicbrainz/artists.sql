{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['artist_mbid'], 'unique': True},
      {'columns': ['artist_name']},
    ]
  )
}}

with artist_release_groups as (
  select
    acn.artist
    , count(distinct rg.id) as release_group_count
    , min(rgm.first_release_date_year) as earliest_release_year
    , max(rgm.first_release_date_year) as latest_release_year
  from {{ source('musicbrainz', 'artist_credit_name') }} as acn
  inner join {{ source('musicbrainz', 'release_group') }} as rg on acn.artist_credit = rg.artist_credit
  inner join {{ source('musicbrainz', 'release_group_meta') }} as rgm on rg.id = rgm.id
  group by acn.artist
)


, artist_tags as (
  select
    artist_tag.artist
    , string_agg(tag.name, ', ' order by artist_tag.count desc, tag.name asc) as tags_string
    , jsonb_agg(
      jsonb_build_object('name', tag.name, 'count', artist_tag.count)
      order by artist_tag.count desc, tag.name asc
    ) as tag_list
  from {{ source('musicbrainz', 'artist_tag') }} as artist_tag
  inner join {{ source('musicbrainz', 'tag') }} as tag on artist_tag.tag = tag.id
  where artist_tag.count > 0
  group by artist_tag.artist
)

select
  a.gid as artist_mbid
  , a.name as artist_name
  , ty.name as artist_type
  , a.comment as disambiguation
  , coalesce(arg.release_group_count, 0) as release_group_count
  , arg.earliest_release_year
  , arg.latest_release_year
  , case
    when arg.earliest_release_year is null then null
    else arg.earliest_release_year::text || '-' || arg.latest_release_year::text
  end as active_years
  , tags.tag_list      -- after active_years
  , tags.tags_string
  , a.last_updated

from {{ source('musicbrainz', 'artist') }} as a
left join {{ source('musicbrainz', 'artist_type') }} as ty on a.type = ty.id
left join artist_release_groups as arg on a.id = arg.artist
left join artist_tags as tags on a.id = tags.artist
