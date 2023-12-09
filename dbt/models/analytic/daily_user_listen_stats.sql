{{ config(materialized='view') }}

{# anaytics on user listens. #}

--- get % of listens that are mapped to a file
with listen_mapped_prop as (
  select
    listens.username
    , listens.listen_at_ts_utc::date as listen_date
    , (
      count(distinct case when map_.filepath is not null then listens.listen_md5 end)::real
      / count(distinct listens.listen_md5)::real
    ) as pct_listens_mapped_to_file

  from {{ ref('listens') }} as listens
  left join {{ ref('file_recording_map') }} as map_ using (recording_mbid)

  where listens.recording_mbid is not null

  group by listens.username, listens.listen_at_ts_utc::date
)

, counts as (
  select
    username
    , listen_at_ts_utc::date as listen_date
    , count(listen_md5) as count_listens
    , count(recording_mbid) as count_recordings
    , count(release_mbid) as count_releases
    , sum(duration_ms) / 3600000::real as sum_listen_hours

  from {{ ref('listens') }}
  where listen_at_ts_utc::date >= current_date - interval '90 days'

  group by username, listen_at_ts_utc::date
)

select
  {{ dbt_utils.generate_surrogate_key(['username', 'listen_date']) }} as user_date_key
  , counts.*
  , listen_mapped_prop.pct_listens_mapped_to_file

from counts
left join listen_mapped_prop using (username, listen_date)

order by counts.username asc, counts.listen_date desc
