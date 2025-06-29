{{ config(materialized='view') }}

select
  dump_record_id
  , slug
  , user_id
  , artist_mbid
  , listen_count

from {{ source('pyingest', 'listenbrainz_data_dump_records') }}
