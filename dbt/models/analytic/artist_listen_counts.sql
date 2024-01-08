{% set ns=[14, 30, 60, 90] %}

with t as (
  select
    listens.username
    , artist_mbid.value::uuid as artist_mbid
    , max(artists.artist_name) as artist_name
    , count(1) as lifetime_listen_count
    , count(distinct listens.recording_mbid) as lifetime_recording_count
    , count(distinct releases.release_group_mbid) as lifetime_release_group_count

    {% for n in ns -%}
      {% set lastn="listens.listen_at_ts_utc >= current_timestamp - interval '%s days'" | format(n) %}
      , count(case when {{ lastn }} then 1 end) as "last{{ n }}_listen_count"
      , count(distinct case when {{ lastn }} then listens.recording_mbid end) as "last{{ n }}_recording_count"
      , count(distinct case when {{ lastn }} then releases.release_group_mbid end) as "last{{ n }}_release_group_count"
    {% endfor %}

  from {{ ref('listens') }} as listens
  left join {{ ref('releases') }} as releases using (release_mbid)
  , jsonb_array_elements_text(listens.artist_mbids) as artist_mbid
  left join {{ ref('artists') }} as artists on artist_mbid.value::uuid = artists.artist_mbid

  where listens.artist_mbids is not null
    and jsonb_array_length(listens.artist_mbids) > 0

  group by 1, 2
)

select
  {{ dbt_utils.generate_surrogate_key(['username', 'artist_mbid']) }} as user_artist_key
  , *

from t
