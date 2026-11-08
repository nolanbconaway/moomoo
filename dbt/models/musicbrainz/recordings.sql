{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['recording_mbid'], 'unique': True},
      {'columns': ['recording_title']},
      {'columns': ['artist_credit_phrase']},
    ]
  )
}}

select
  r.gid as recording_mbid
  , r.name as recording_title
  , r.length as recording_length_ms
  , ac.name as artist_credit_phrase
  , r.last_updated as last_updated

from {{ source('musicbrainz', 'recording') }} as r
inner join {{ source('musicbrainz', 'artist_credit') }} as ac on ac.id = r.artist_credit