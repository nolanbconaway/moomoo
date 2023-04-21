{{ config(materialized='view') }}

{# Albums from artists without a lot of listens, ranked by similar user score. #}

with artist_listen_counts as (
    select
      username
      , artist_mbid.value::uuid as artist_mbid
      , count(distinct recording_mbid) as listen_count

    from {{ ref('listens_flat') }} as listens_flat
      , jsonb_array_elements_text(listens_flat.artist_mbids) as artist_mbid

    where listens_flat.artist_mbids is not null
      and jsonb_array_length(listens_flat.artist_mbids) > 0

    group by 1, 2
)

, release_scores as (
    select
      s.username
      , s.time_range
      , concat(r.release_title, ' - ', r.artist_credit_phrase) as description_text
      , row_number() over (partition by s.username, s.time_range order by s.score desc) as rank
    from {{ ref('similar_user_recommends') }} as s
    inner join {{ ref('dim_release') }} as r
      on r.release_mbid = s.mbid
        and s.entity = 'release'

    left join artist_listen_counts as alc
      on alc.artist_mbid = any(r.artist_mbids_list)
        and alc.username = s.username
        and alc.listen_count < 5

    where alc.artist_mbid is null
)

select
    username
    , rank
    , max(case when time_range = 'all_time' then description_text end) as "all_time"
    , max(case when time_range = 'year' then description_text end) as "year"
    , max(case when time_range = 'month' then description_text end) as "month"

from release_scores

where rank <= 50

group by 1, 2
order by 1, 2
