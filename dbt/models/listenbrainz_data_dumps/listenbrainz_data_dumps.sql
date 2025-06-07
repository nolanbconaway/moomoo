{{ config(materialized='view') }}

select
  slug
  , ftp_path
  , ftp_modify_ts
  , date
  , start_timestamp
  , end_timestamp
  , created_at
  , refreshed_at

from {{ source('pyingest', 'listenbrainz_data_dumps') }}
