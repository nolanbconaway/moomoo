{{ config(materialized='view') }}

{# expose source data #}

select
  filepath
  , success
  , fail_reason
  , duration_seconds
  , embedding
  , conditioned_embedding
  , insert_ts_utc
from {{ source('pyingest', 'local_music_embeddings') }}

-- remove rows where the conditioned embedding is null due to the model not having run.
-- allow cases with nulls due success being false
where conditioned_embedding is not null or success = false
