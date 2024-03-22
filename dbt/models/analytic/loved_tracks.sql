{# 
  Tracks that users love.

  Unions the spikes against the explicit loves.  
#}

with recordings as (
  select username, recording_mbid, feedback_at as love_at
  from {{ ref('listenbrainz_feedback') }}

  union all

  select username, recording_mbid, period_start_at_utc as love_at
  from {{ ref('track_play_spikes') }}
)

select
  recordings.username
  , file_map.filepath
  , array_agg(distinct recordings.recording_mbid) as recording_mbids
  , min(recordings.love_at) as love_at

from recordings
inner join {{ ref('map__file_recording') }} as file_map using (recording_mbid)
group by recordings.username, file_map.filepath
order by recordings.username, min(recordings.love_at) desc
