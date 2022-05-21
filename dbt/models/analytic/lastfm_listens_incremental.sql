{{ config(
    indexes=[
      {'columns': ['lastfm_entity_id', 'to_pyingest_id'], 'unique': True},
      {'columns': ['lastfm_entity_id']},
      {'columns': ['to_ts_nyc']}
    ]
)}}

{# 
  Count entity plays per increment of time. This uses the overall payload and the fact 
  that I capture data once per day in an airflow job, so I can take deltas.

  Ensure that the periods will always be on different days, but no guarantee that they
  will be on consecutive days.
#}

with ingest_pairs as (
  select
    pyingest_id as this_pyingest_id
    , ts_nyc as this_ts_nyc
    , lag(pyingest_id) over (partition by kind order by ts_utc) as prev_pyingest_id
    , lag(ts_nyc) over (partition by kind order by ts_utc) as prev_ts_nyc

  from {{ ref('ingests') }}
  where ingests."period" = 'overall'
    and ingests.date_nyc_ingest_index = 1
)

select
  this.lastfm_entity_id
  , ingest_pairs.prev_pyingest_id as from_pyingest_id
  , ingest_pairs.this_pyingest_id as to_pyingest_id

  , ingest_pairs.prev_ts_nyc as from_ts_nyc
  , ingest_pairs.this_ts_nyc as to_ts_nyc
  , ingest_pairs.this_ts_nyc - ingest_pairs.prev_ts_nyc as period_length
  
  , coalesce(prev.playcount, 0) as from_playcount
  , this.playcount as to_playcount
  , this.playcount - coalesce(prev.playcount, 0) as incremental_playcount

from ingest_pairs 
join {{ ref('lastfm_payloads_flat') }} as this
  on this.pyingest_id = ingest_pairs.this_pyingest_id
left join {{ ref('lastfm_payloads_flat') }} as prev
  on prev.pyingest_id = ingest_pairs.prev_pyingest_id
    and prev.lastfm_entity_id = this.lastfm_entity_id

where coalesce(prev.playcount, 0) < this.playcount
  and ingest_pairs.prev_pyingest_id is not null
