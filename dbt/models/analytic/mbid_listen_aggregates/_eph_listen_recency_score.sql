{{ config(materialized = 'ephemeral') }}
{# Ephemeral model scoring listens for recency, for use in downstream agg models. #}

{# Define recency formula or re use later. #}
{% set recency='extract(day from current_timestamp - listens.listen_at_ts_utc)' %}

select
  listens.username
  , listens.recording_mbid
  , listens.release_mbid
  , releases.release_group_mbid
  , listens.artist_mbids
  , listens.listen_at_ts_utc
  , {{ recency }} as listen_recency_days

  -- exp(-0.1 * days) sets up ~50% at 7 days, ~5% at 30 days, ~0.2% at 60 days
  , exp(-0.1 * {{ recency }})::numeric as recency_pct

from {{ ref('listens') }} as listens
inner join {{ ref('releases') }} as releases using (release_mbid)

where listens.recording_mbid is not null
  and listens.release_mbid is not null
  and listens.artist_mbids is not null
