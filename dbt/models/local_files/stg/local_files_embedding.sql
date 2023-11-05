{{ config(materialized='view') }}

{# expose source data #}

select
  filepath
  , success
  , fail_reason
  , duration_seconds
  , embedding
  , insert_ts_utc
from {{ source('pyingest', 'local_music_embeddings') }}
