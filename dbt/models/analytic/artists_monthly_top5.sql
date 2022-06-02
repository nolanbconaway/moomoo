{{ config(materialized='view') }}

with counted as (
  select
      date_trunc('month', lastfm_listens_incremental.to_ts_nyc)::date as month_
      , lastfm_entities.lastfm_entity_id
      , max(lastfm_entities.name) as artist_name
      , sum(lastfm_listens_incremental.incremental_playcount) as playcount

  from {{ ref('lastfm_listens_incremental') }}
  join {{ ref('lastfm_entities') }}
    on lastfm_entities.lastfm_entity_id = lastfm_listens_incremental.lastfm_entity_id

  where lastfm_entities.kind = 'artists' 
    and (
      date_trunc('month', lastfm_listens_incremental.to_ts_nyc) 
      < date_trunc('month', current_date)
    )
      
  group by 1, 2
)

, ordered as (
  select *, row_number() over (partition by month_ order by playcount desc) as rank_
  from counted
)

select 
  month_
  , max(case when rank_ = 1 then concat(artist_name, ' (', playcount, ')') end) as rank_1
  , max(case when rank_ = 2 then concat(artist_name, ' (', playcount, ')') end) as rank_2
  , max(case when rank_ = 3 then concat(artist_name, ' (', playcount, ')') end) as rank_3
  , max(case when rank_ = 4 then concat(artist_name, ' (', playcount, ')') end) as rank_4
  , max(case when rank_ = 5 then concat(artist_name, ' (', playcount, ')') end) as rank_5

from ordered

where rank_ <= 5

group by 1

