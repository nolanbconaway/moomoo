{{ config(materialized='view') }}

{# anaytics on user listens. #}

with t as (
  select
    listens.username
    , listens.listen_at_ts_utc::date as date
    , count(distinct listens.listen_md5) as count_listens
    , (
      count(distinct case when map_.filepath is not null then listens.listen_md5 end)::real
      / count(distinct listens.listen_md5)::real
    ) as pct_listens_mapped_to_file
    , count(distinct listens.recording_mbid) as count_recordings
    , count(distinct listens.release_mbid) as count_releases
    , sum(distinct listens.duration_ms) / 3600000::real as sum_listen_hours

  from {{ ref('listens') }} as listens
  -- NOTE: one to many here. 1 recording can have 0-2 files.
  -- eventually split this into a cte and join back in.
  left join {{ ref('file_recording_map') }} as map_ using (recording_mbid)

  where listens.recording_mbid is not null
    and listens.listen_at_ts_utc::date >= current_date - interval '90 days'

  group by 1, 2
)

select 
  {{ dbt_utils.generate_surrogate_key(['username', 'date']) }} as user_date_key
  , *

from t
order by 1, 2 desc
