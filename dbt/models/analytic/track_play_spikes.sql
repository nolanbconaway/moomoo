{{ config(materialized='view') }}

-- list all listens that had >= 5 listens in the proceeding 24h. there can be overlap
-- between the listen periods if e.g., there are > 5 listens.
--
-- we will reduce to one track spike per period later.
with spikes_unconstrained as (
  select
    t0.listen_md5
    , min(t0.username) as username
    , min(t0.listen_at_ts_utc) as period_start_at_utc
    , {{ dbt_utils.surrogate_key(['min(t0.username)', 'min(t0.recording_mbid)']) }} as user_recording_mbid
    , count(*) as next_24h_listen_count

  from {{ ref('listens_flat') }} as t0
  inner join {{ ref('listens_flat') }} as tn
    on tn.recording_mbid = t0.recording_mbid
      and t0.recording_mbid is not null
      and tn.username = t0.username
      and tn.listen_at_ts_utc < (t0.listen_at_ts_utc + interval '1 day')
      and tn.listen_at_ts_utc > t0.listen_at_ts_utc

  group by t0.listen_md5
  having count(*) >= 5
)

-- make a list of listen md5s to EXCLUDE; (e.g., duplicates).
--
-- exclude any period within 24h of a same track period with less listens.
-- this is to avoid multiple spikes for the same track within a 24h period.
-- if a tie, then use the first period
, exclude as (
  select tx.listen_md5
  from spikes_unconstrained as t0
  inner join spikes_unconstrained as tx
    on tx.user_recording_mbid = t0.user_recording_mbid
      and abs(extract(hour from tx.period_start_at_utc - t0.period_start_at_utc)) < 24

  where
    case
      when tx.next_24h_listen_count < t0.next_24h_listen_count then true
      when tx.next_24h_listen_count > t0.next_24h_listen_count then false
      else tx.period_start_at_utc > t0.period_start_at_utc
    end
)

select
  spikes_unconstrained.listen_md5 as start_listen_md5
  , listens_flat.recording_mbid
  , spikes_unconstrained.username
  , spikes_unconstrained.period_start_at_utc
  , spikes_unconstrained.next_24h_listen_count
  , listens_flat.track_name
  , listens_flat.release_name
  , listens_flat.artist_name

from spikes_unconstrained
inner join {{ ref('listens_flat') }} as listens_flat
  on spikes_unconstrained.listen_md5 = listens_flat.listen_md5

where spikes_unconstrained.listen_md5 not in (select * from exclude)

order by spikes_unconstrained.period_start_at_utc desc
