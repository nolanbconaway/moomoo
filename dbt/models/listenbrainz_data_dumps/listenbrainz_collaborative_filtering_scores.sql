{{ config(materialized='view') }}

select
    artist_mbid_a as artist_mbid_a,
    artist_mbid_b as artist_mbid_b,
    score_value,
    insert_ts_utc
from {{ source('pyingest', 'listenbrainz_collaborative_filtering_scores') }}

union all

select
    artist_mbid_b as artist_mbid_a,
    artist_mbid_a as artist_mbid_b,
    score_value,
    insert_ts_utc

from {{ source('pyingest', 'listenbrainz_collaborative_filtering_scores') }}
