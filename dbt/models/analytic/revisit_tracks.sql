{{
    config(
      materialized='table',
      indexes=[
          {'columns': ['recording_mbid', 'username'], 'unique': True},
          {'columns': ['username']},
          {'columns': ['artist_mbid']},
          {'columns': ['album_artist_mbid']},
        ]
    )
}}

{# Create a list of revisitable tracks per username.

  This involves some custom logic to detect a track that has historically been listened
  to a lot, but not recently.

  In particluar, the measurements are:

   - listens_old: the number of listens older than 90 days
   - listens_recent: the number of listens in the last 90 days
#}

with recordings as (
  select
    recording_mbid
    , recording_title
    , artist_credit_phrase as artist_name
    , username
    , revisit_score
    , last150_listen_count - last90_listen_count as listens_old
    , last90_listen_count as listens_recent

  from {{ ref('recording_listen_counts') }}

  where last90_listen_count <= 2 -- only a few listens in the last 90 days
    and last14_listen_count = 0 -- no listens in the last 14 days
    and (last150_listen_count - last90_listen_count) >= 2 -- at least 2 listens older than 90 days

    -- baseline revisit score of 30 days recency * 3 listens = (1 - 0.05) * ln(3)
    and revisit_score > 1.05
)

-- attach a file, artist mbids. use distinct on to select a single file for each recording
select distinct on (recordings.recording_mbid, recordings.username)
  recordings.recording_mbid
  , recordings.username
  , local_files.filepath
  , recordings.recording_title
  , recordings.artist_name
  , local_files.artist_mbid
  , local_files.album_artist_mbid
  , recordings.revisit_score
  , recordings.listens_old
  , recordings.listens_recent

from recordings
inner join {{ ref('map__file_recording') }} using (recording_mbid)
inner join {{ ref('local_files') }} as local_files using (filepath)

where local_files.artist_mbid is not null

-- use a stable sort to ensure we get the same file for each recording, but semi-randomly
order by recordings.recording_mbid, recordings.username, md5(local_files.filepath)
