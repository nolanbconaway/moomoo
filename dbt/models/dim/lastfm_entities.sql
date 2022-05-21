{{ config(indexes=[{'columns': ['lastfm_entity_id'], 'unique': True}]) }}

{# 
    Dimensional model for entities
#}

select distinct
    lastfm_entity_id
    , kind
    , name
    , url
    , mbid
    , artist__name
    , first_value(ts_utc) over (partition by lastfm_entity_id order by ts_utc) as first_ts_utc

from {{ ref('lastfm_payloads_flat') }}

