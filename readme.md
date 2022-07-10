# Nolan's LastFM Music Recommendation System.

[![Unit Tests](https://github.com/nolanbconaway/lastfm-recommender/actions/workflows/push.yml/badge.svg)](https://github.com/nolanbconaway/lastfm-recommender/actions/workflows/push.yml)

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use the LastFM API to get what I need.

## Status

This is ongoing! 

Current status: I wrote the dbt models for providing easy access to my data, and next i need to toy with 
models for recommendation. 

## Setup 

- install: `pip install -e .`
- CLI entrypoint: `lastfmrec`

### Env Requires

I have a .env like:

```
LASTFM_API_KEY="xxxxxxxx"
POSTGRES_DSN="dbname=my_db ..."
```

These scripts assume those variables are exported.

### Database

This postgres table stores the raw data obtained from the Last FM API. My goal is to run a script daily to append values here.

> TODO: db diagram

#### DBT

If you have data being populated into the db; you can use the models in [dbt](dbt/) to expose those json payloads into useful/structured data.
