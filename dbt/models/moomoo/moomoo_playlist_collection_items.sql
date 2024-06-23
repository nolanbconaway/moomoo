{{ config(materialized='view') }}

select
  playlist_id
  , collection_id
  , collection_order_index
  , title
  , description
  , playlist
  , create_at_utc

from {{ source('pyingest', 'moomoo_playlist_collection_items') }}
