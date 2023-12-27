{{ config(materialized='view') }}

select
  feedback_md5
  , username
  , score
  , recording_mbid
  , feedback_at
  , insert_ts_utc as _ingest_insert_ts_utc

from {{ source('pyingest', 'listenbrainz_user_feedback') }}
