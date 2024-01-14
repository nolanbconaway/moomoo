{{ config(materialized='view') }}

{# Create a list of revisitable releases per username.

  This involves some custom logic to detect a release that has historically been 
  listened to a lot, but not recently.

  In particluar, the measurements are:

   - listens_old: the number of listens in the last 180-90 days
   - listens_recent: the number of listens in the last 90 days
   - num_recordings: the number of recordings in the release group

  This model identifies release groups with more listens than recordings in the past,
  but few listens recently.

#}

{% set ts="current_timestamp - interval '90 days'" %}

with release_group_stats as (
  select
    releases.release_group_mbid
    , listens.username
    , count(distinct listens.recording_mbid) as num_recordings
    , sum(case when listens.listen_at_ts_utc < {{ ts }} then 1 end) as listens_old
    , sum(case when listens.listen_at_ts_utc >= {{ ts }} then 1 end) as listens_recent

  from {{ ref('listens') }} as listens
  inner join {{ ref('releases') }} as releases using (release_mbid)

  group by 1, 2
  -- idk about singles or huge box sets
  having count(distinct listens.recording_mbid) between 4 and 20
)

select
  stats_.release_group_mbid
  , release_groups.release_group_title as release_group_title
  , release_groups.artist_credit_phrase as artist_name
  , stats_.username
  , stats_.num_recordings
  , stats_.listens_old
  , stats_.listens_recent

from release_group_stats as stats_
inner join {{ ref('release_groups') }} as release_groups using (release_group_mbid)
where stats_.listens_old > (stats_.num_recordings * 2)
  and stats_.listens_old > 10
  and stats_.listens_recent < 10
