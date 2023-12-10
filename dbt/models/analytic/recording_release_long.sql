with t as (
  select
    recordings.recording_mbid
    , {{ try_cast_uuid(json_get('release_list.value', ["id"])) }} as release_mbid

  from {{ ref('recordings') }} as recordings
  , jsonb_array_elements(recordings.release_list) as release_list
)

select
  {{ dbt_utils.generate_surrogate_key(['t.recording_mbid', 't.release_mbid']) }} as recording_release_key
  , t.recording_mbid
  , t.release_mbid
  , releases.release_group_mbid

from t
inner join {{ ref('releases') }} as releases using (release_mbid)
