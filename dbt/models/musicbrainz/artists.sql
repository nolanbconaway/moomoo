{{ config(
    materialized='incremental',
    unique_key='artist_mbid',
    indexes=[
      {'columns': ['artist_mbid'], 'unique': True},
      {'columns': ['artist_name']},
      {'columns': ['_ingest_insert_ts_utc']},
      ],
    )
}}


with base_ as (
  select
    mbid as artist_mbid
    , {{ json_get('payload_json', ["data", "artist", "name"]) }}::varchar as artist_name
    , {{ json_get('payload_json', ["data", "artist", "type"]) }}::varchar as artist_type
    , {{ json_get('payload_json', ["data", "artist", "disambiguation"]) }}::varchar as disambiguation
    , {{ json_get('payload_json', ["data", "artist", "alias-list"], as_json=True) }} as alias_list
    , {{ json_get('payload_json', ["data", "artist", "url-relation-list"], as_json=True) }} as url_relation_list
    , {{ json_get('payload_json', ["data", "artist", "artist-relation-list"], as_json=True) }} as artist_relation_list
    , {{ json_get('payload_json', ["data", "artist", "release-list"], as_json=True) }} as release_list
    , {{ json_get('payload_json', ["data", "artist", "tag-list"], as_json=True) }} as tag_list
    , {{ json_get('payload_json', ["data", "artist", "release-count"]) }}::int as release_count
    , ts_utc as _ingest_insert_ts_utc

  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'artist'
    and {{ json_get('payload_json', ['_success']) }} = 'true'

    {% if is_incremental() %}
    and ts_utc > (select max(_ingest_insert_ts_utc) - interval '5 minutes' from {{ this }})
    {% endif %}
)

, release_timeline as (
  select
    base_.artist_mbid
    , max({{ extract_year(json_get('release_.value', ["date"])) }}) as latest_release_year
    , min({{ extract_year(json_get('release_.value', ["date"])) }}) as earliest_release_year

  from base_
  , jsonb_array_elements(base_.release_list) as release_
  group by 1
)

, tags_str as (
  select
    base_.artist_mbid
    , string_agg(
      {{ json_get('tag.value', ["name"]) }}
      , ', '
      order by {{ json_get('tag.value', ["count"]) }}::int desc
    ) as tags

  from base_
  , jsonb_array_elements(base_.tag_list) as tag
  group by 1
)

select
  artist_mbid
  , artists.artist_name
  , artists.artist_type
  , artists.disambiguation
  , artists.alias_list
  , artists.url_relation_list
  , artists.artist_relation_list
  , artists.release_list
  , artists.tag_list
  , artists.release_count
  , release_timeline.earliest_release_year
  , release_timeline.latest_release_year
  , case
    when release_timeline.earliest_release_year is null then null
    else release_timeline.earliest_release_year || '-' || release_timeline.latest_release_year
  end as active_years

  , tags_str.tags as tags_string
  , artists._ingest_insert_ts_utc

from base_ as artists
left join release_timeline using (artist_mbid)
left join tags_str using (artist_mbid)
