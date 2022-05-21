{{ config(indexes=[{'columns': ['pyingest_id'], 'unique': True}]) }}

{# 
    Dimensional model for ingest actions. I listen in nyc time so lets use that.
#}

select
    {{ dbt_utils.surrogate_key(['ts_utc', 'kind', 'period']) }} as "pyingest_id"
    , ts_utc
    , ts_utc at time zone 'america/new_york' as ts_nyc
    , kind
    , "period"
    , row_number() over (
        partition by (ts_utc at time zone 'america/new_york')::date, kind 
        order by ts_utc
      ) as "date_nyc_ingest_index"

    

from {{ source('pyingest', 'lastfm') }}

