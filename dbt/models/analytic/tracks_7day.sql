{{ config(materialized='view') }}

select
    lastfm_entities.lastfm_entity_id
    , max(lastfm_entities.name) as track_name
    , max(lastfm_entities.artist__name) as artist_name
    , sum(lastfm_listens_incremental.incremental_playcount) as playcount

from {{ ref('lastfm_listens_incremental') }}
join {{ ref('lastfm_entities') }}
  on lastfm_entities.lastfm_entity_id = lastfm_listens_incremental.lastfm_entity_id


where lastfm_listens_incremental.to_ts_nyc::date >= current_date - interval '7 day'
  and lastfm_entities.kind = 'tracks'
    
group by lastfm_entities.lastfm_entity_id