{{
    config(
      materialized='table',
      indexes=[
          {'columns': ['release_group_mbid'], 'unique': True},
          {'columns': ['username']},
        ]
    )
}}

{# Create a list of revisitable releases per username.

  This involves some custom logic to detect a release that has historically been 
  listened to a lot, but not recently.

  In particluar, the measurements are:

   - listens_old: the number of listens older than 90 days
   - listens_recent: the number of listens in the last 90 days
   - num_recordings: the number of recordings in the release group

  This model identifies release groups with more listens than recordings in the past,
  but few listens recently.
#}

select
  release_group_mbid
  , release_group_title
  , artist_credit_phrase as artist_name
  , username
  , revisit_score
  , lifetime_recording_count as num_recordings
  , last150_listen_count - last90_listen_count as listens_old
  , last90_listen_count as listens_recent

from {{ ref('release_group_listen_counts') }}

where lifetime_recording_count between 4 and 20 -- idk about singles or huge box sets
  and last30_listen_count < 5 -- not a lot of listens in the last 30 days
  and (last150_listen_count - last90_listen_count) >= 5 -- at least 5 listens older than 90 days
  and (last150_listen_count - last90_listen_count) >= (lifetime_recording_count * 1.5) -- more listens than recordings

  -- baseline revisit score of 30 days recency * 3 tracks = (1 - 0.05) * ln(3)
  and revisit_score > 1.05
order by revisit_score desc
