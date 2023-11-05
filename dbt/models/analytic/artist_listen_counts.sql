{% set last30="listens.listen_at_ts_utc >= current_timestamp - interval '30 day'" %}

with t as (
  select
    listens.username
    , artist_mbid.value::uuid as artist_mbid
    , count(1) as lifetime_listen_count
    , count(distinct listens.recording_mbid) as lifetime_recording_count
    , count(distinct releases.release_group_mbid) as lifetime_release_group_count
    , count(case when {{ last30 }} then 1 end) as last30_listen_count
    , count(distinct case when {{ last30 }} then listens.recording_mbid end) as last30_recording_count
    , count(distinct case when {{ last30 }} then releases.release_group_mbid end) as last30_release_group_count

  from {{ ref('listens') }} as listens
  left join {{ ref('releases') }} as releases using (release_mbid)

  , jsonb_array_elements_text(listens.artist_mbids) as artist_mbid

  where listens.artist_mbids is not null
    and jsonb_array_length(listens.artist_mbids) > 0

  group by 1, 2
)

select
  {{ dbt_utils.generate_surrogate_key(['username', 'artist_mbid']) }} as user_artist_key
  , *

from t
