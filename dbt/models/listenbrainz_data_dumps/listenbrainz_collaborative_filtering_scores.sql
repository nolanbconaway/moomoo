{{ config(materialized='view') }}

select
  artist_mbid_a
  , artist_mbid_b
  , score_value
  , insert_ts_utc

from {{ source('pyingest', 'listenbrainz_collaborative_filtering_scores') }}

-- make sure the diagonal is only added once
where artist_mbid_a != artist_mbid_b

union all

select
  artist_mbid_b as artist_mbid_a
  , artist_mbid_a as artist_mbid_b
  , score_value
  , insert_ts_utc

from {{ source('pyingest', 'listenbrainz_collaborative_filtering_scores') }}
