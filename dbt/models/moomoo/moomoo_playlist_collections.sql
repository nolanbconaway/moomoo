{{ config(materialized='view') }}

select
  collection_id
  , collection_name
  , username
  , refresh_at_hours_utc
  , create_at_utc
  , refreshed_at_utc

from {{ source('pyingest', 'moomoo_playlist_collections') }}
