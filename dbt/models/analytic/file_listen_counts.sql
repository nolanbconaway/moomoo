{{ config(materialized='view') }}

{# historical listen statistics per file. #}


with listen_file_map as (
  select
    listens.listen_md5
    , map_.filepath
    , count(1) over (partition by listens.listen_md5) as potential_file_count

  from {{ ref('listens') }} as listens
  inner join {{ ref('map__file_recording') }} as map_ using (recording_mbid)

  where listens.recording_mbid is not null
)

{% set last7="listens.listen_at_ts_utc >= current_timestamp - interval '7 day'" %}
{% set last30="listens.listen_at_ts_utc >= current_timestamp - interval '30 day'" %}
{% set last90="listens.listen_at_ts_utc >= current_timestamp - interval '90 day'" %}
{% set listen_count = '1::real / map_.potential_file_count' %}

, counts as (
  select
    map_.filepath
    , listens.username
    , max(files.track_name) as track_name
    , max(files.album_name) as album_name
    , max(files.artist_name) as artist_name
    , max(files.album_artist_name) as album_artist_name
    , sum({{ listen_count }}) as lifetime_listen_count
    , sum(case when {{ last7 }} then {{ listen_count }} else 0 end) as last07_listen_count
    , sum(case when {{ last30 }} then {{ listen_count }} else 0 end) as last30_listen_count
    , sum(case when {{ last90 }} then {{ listen_count }} else 0 end) as last90_listen_count


  from listen_file_map as map_
  inner join {{ ref('listens') }} as listens using (listen_md5)
  inner join {{ ref('local_files') }} as files using (filepath)

  group by map_.filepath, listens.username
)

select
  {{ dbt_utils.generate_surrogate_key(['filepath', 'username']) }} as filepath_username_id
  , *

from counts
