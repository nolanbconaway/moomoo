# Nolan's LastFM Music Recommendation System.

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use the LastFM API to get what I need.

## Status

- [x] LastFM integrated with my music players
- [x] Data being collected to my postgres db
- [ ] Tools to build playlists/recs on the basis of said data.

## Setup 

- install: `pip install -e .`
- data collection script: `python -m lastfmrec.collect_data`

### Env Requires

I have a .env like:

```
LASTFM_USERNAME="username"
LASTFM_API_KEY="xxxxxxxx"
POSTGRES_DSN="dbname=my_db ..."
```

These scripts assume those variables are exported.

### Database

This table stores the raw data obtained from the Last FM API. My goal is to run a script daily to append values here.

```sql
create table lastfm (
    "ts_utc" timestamp with time zone default CURRENT_TIMESTAMP not null
    , "kind" varchar not null
    , "period" varchar not null
    , "json_data" jsonb not null
);

create index on lastfm (ts_utc, kind, period);
```

I just built it once manually because I don't care about Future Nolan.
