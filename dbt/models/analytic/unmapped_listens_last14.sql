{{ config(materialized='view') }}

{# List out listens in the last 14 days without a mapped file. #}

select listens.*

from {{ ref('listens') }} as listens
left join {{ ref('map__file_recording') }} as map_ using (recording_mbid)

where listens.recording_mbid is not null
  and map_.filepath is null
  and listens.listen_at_ts_utc > current_timestamp - interval '14 days'
