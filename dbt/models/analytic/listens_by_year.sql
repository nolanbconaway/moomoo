{{ config(materialized='view') }}

-- count user listens for recordings in each year

with by_year as (
  select
    listens.username
    , recording.release_year
    , count(*) as listens

  from {{ ref('listens_flat') }} as listens
  inner join {{ ref('dim_recording') }} as recording
    on listens.recording_mbid = recording.recording_mbid
      and recording.release_year is not null

  group by 1, 2
)

, totals as (
  select
    username
    , sum(listens) as total_listens

  from by_year

  group by 1
)

select
  by_year.username
  , by_year.release_year
  , by_year.listens
  , by_year.listens::real / totals.total_listens as listens_pct

from by_year
inner join totals on by_year.username = totals.username

order by 1, 2 desc
