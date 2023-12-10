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

with release_group_stats as (
  select
    releases.release_group_mbid
    , listens.username
    , count(distinct listens.recording_mbid) as num_recordings
    , sum(
      case
        when
          listens.listen_at_ts_utc between current_timestamp - interval '180 days'
          and current_timestamp - interval '90 days'
          then 1
      end
    ) as listens_old
    , sum(
      case
        when listens.listen_at_ts_utc >= current_timestamp - interval ' 90 days'
          then 1
      end
    ) as listens_recent
  from {{ ref('listens') }} as listens
  inner join {{ ref('releases') }} as releases using (release_mbid)

  group by 1, 2
  having count(distinct listens.recording_mbid) between 4 and 20 --- idk about singles
)

select release_group_mbid, username

from release_group_stats
where listens_old > (num_recordings + 5)
  and listens_old > 10
  and listens_recent < 10
