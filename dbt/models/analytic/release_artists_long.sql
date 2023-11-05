with t as (
  select distinct -- found a dupe once
    release_groups.release_group_mbid
    , {{ try_cast_uuid(json_get('artist_credits.value', ["artist", "id"])) }} as artist_mbid

  from {{ ref('release_groups') }} as release_groups
  , jsonb_array_elements(release_groups.artist_credit_list) as artist_credits
)

select
  {{ dbt_utils.generate_surrogate_key(['release_group_mbid', 'artist_mbid']) }} as release_group_artist_key
  , release_group_mbid
  , artist_mbid
from t
where artist_mbid is not null
