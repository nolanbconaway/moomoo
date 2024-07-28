{% set ns=[14, 30, 60, 90, 120, 150] %}

with t as (
  select
    listens.username
    , recordings.recording_mbid
    , max(recordings.recording_title) as recording_title
    , max(recordings.artist_credit_phrase) as artist_credit_phrase

    -- recency and revisit scores
    , min(listens.listen_recency_days) as listen_recency_days
    , round(avg(listens.listen_recency_days)) as avg_listen_recency_days
    , round(sum(listens.recency_pct), 5) as recency_score
    , exp(sum(ln(listens.inv_recency_pct))) * ln(count(1) + 1) as revisit_score

    -- listen counts
    , count(1) as lifetime_listen_count

    {% for n in ns -%}
      {% set lastn="listens.listen_at_ts_utc >= current_timestamp - interval '%s days'" | format(n) %}
      , count(case when {{ lastn }} then 1 end) as "last{{ n }}_listen_count"
    {% endfor %}

  from {{ ref('_eph_listen_recency_score') }} as listens
  inner join {{ ref('recordings') }} as recordings using (recording_mbid)

  group by 1, 2
)

select
  {{ dbt_utils.generate_surrogate_key(['username', 'recording_mbid']) }} as user_recording_key
  , *

from t
