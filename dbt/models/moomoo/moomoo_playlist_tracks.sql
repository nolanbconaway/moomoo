{{ config(materialized='view') }}

select
  track_id
  , playlist_id
  , track_order_index
  , filepath
  , recording_mbid
  , release_mbid
  , release_group_mbid
  , artist_mbid
  , album_artist_mbid
  , track_length_seconds
  , match_distance
  , is_seed

from {{ source('pyingest', 'moomoo_playlist_tracks') }}
