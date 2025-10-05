{{ config(
  indexes=[
    {'columns': ['artist_mbid'], 'unique': True},
    {'columns': ['artist_name']},
    {'columns': ['_ingest_insert_ts_utc']},
    ]
  ) }}


with base_ as (
  select
    mbid as artist_mbid
    , {{ json_get('payload_json', ["data", "artist", "name"]) }}::varchar as artist_name
    , {{ json_get('payload_json', ["data", "artist", "type"]) }}::varchar as artist_type
    , {{ json_get('payload_json', ["data", "artist", "disambiguation"]) }}::varchar as disambiguation
    , {{ json_get('payload_json', ["data", "artist", "alias-list"], as_json=True) }} as alias_list
    , {{ json_get('payload_json', ["data", "artist", "url-relation-list"], as_json=True) }} as url_relation_list
    , {{ json_get('payload_json', ["data", "artist", "artist-relation-list"], as_json=True)}} as artist_relation_list
    , {{ json_get('payload_json', ["data", "artist", "release-list"], as_json=True) }} as release_list
    , {{ json_get('payload_json', ["data", "artist", "tag-list"], as_json=True) }} as tag_list
    , {{ json_get('payload_json', ["data", "artist", "release-count"]) }}::int as release_count
    , ts_utc as _ingest_insert_ts_utc


  from {{ source('pyingest', 'musicbrainz_annotations') }}

  where entity = 'artist'
    and {{ json_get('payload_json', ['_success']) }} = 'true'
)
, release_timeline as (
  select
    artists.artist_mbid
    , max({{ extract_year(json_get('release.value', ["date"])) }}) as latest_release_year
    , min({{ extract_year(json_get('release.value', ["date"])) }}) as earliest_release_year

  from base_ as artists, jsonb_array_elements(artists.release_list) as release
  group by 1
)


, tags_str as (
  select
    artist_mbid
    , string_agg(tag.value->>'name', ', ' order by (tag.value->>'count')::int desc) as tags

  from base_, jsonb_array_elements(tag_list) as tag
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
  , tags_str.tags as tags_string
  , artists._ingest_insert_ts_utc


from base_ as artists
left join release_timeline using (artist_mbid)
left join tags_str using (artist_mbid)
