
{{ config(
    indexes=[
      {'columns': ['lastfm_entity_id', 'pyingest_id'], 'unique': True},
      {'columns': ['ts_utc']},
    ]
)}}

{# 
    Unpack JSON to rows. Most other models will be aggregates on this.
#}

with t as (
    select
        distinct -- i found some odd cases of duplicates in the raw JSON
        {{ dbt_utils.surrogate_key(['ts_utc', 'kind', 'period']) }} as "pyingest_id"
        , "ts_utc"
        , "kind"
        , "period"
        , record.value ->> 'url' as "url"
        , nullif(record.value ->> 'mbid', '') as "mbid"
        , record.value ->> 'name' as "name"
        , (record.value ->> 'playcount')::int as "playcount"
        , (record.value ->> 'duration')::int as "duration"
        , nullif(record.value -> 'artist' ->> 'url', '') as "artist__url"
        , nullif(record.value -> 'artist' ->> 'mbid', '') as "artist__mbid"
        , nullif(record.value -> 'artist' ->> 'name', '') as "artist__name"

    from {{ source('pyingest', 'lastfm') }},
        jsonb_array_elements(json_data) as record
)


select
    {{ dbt_utils.surrogate_key(['kind', 'name', 'url', 'mbid']) }} as "lastfm_entity_id"
    , "pyingest_id"
    , "ts_utc"
    , "kind"
    , "period"
    , "url"
    , "mbid"
    , "name"
    , "playcount"
    , "duration"
    , case when "kind"='artists' then "url" else "artist__url" end as "artist__url"
    , case when "kind"='artists' then "mbid" else "artist__mbid" end as "artist__mbid"
    , case when "kind"='artists' then "name" else "artist__name" end as "artist__name"

from t