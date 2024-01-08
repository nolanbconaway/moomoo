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

, dedupe as (
  select username, recording_mbid, min(love_at) as love_at
  from recordings
  group by username, recording_mbid
)

select distinct recordings.username, file_map.filepath, recordings.love_at
from dedupe as recordings
inner join {{ ref('map__file_recording') }} as file_map using (recording_mbid)
order by recordings.username, recordings.love_at
