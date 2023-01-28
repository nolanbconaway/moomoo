# `moomoo`: Nolan's Homemade Music Recommendation System.

[![Unit Tests](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml)

![](https://archives.bulbagarden.net/media/upload/5/5f/MooMoo_Farm_anime.png)

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use open source software to get what I need.

Specifically, I'll be:

- Scrobbling my listening behavior to [ListenBrainz](https://listenbrainz.org/)
- Enriching media via the [MusicBrainz](https://musicbrainz.org/) API
- Providing analytics on what I've listened to via [dbt](dbt/)
- Working on a recommender system for music I haven't heard yet.

## Status

This is ongoing! 

Current status: Writing ingestion pipeline and DBT models to curate a detailed dataset on my listening behavior.

## Setup 

- install: `pip install -e .`
- CLI entrypoint: `moomoo`

### Env Requires

I have a .env like:

```
POSTGRES_DSN="dbname=my_db ..."
CONTACT_EMAIL="me@email.com"  # identify with musicbrainz
```

## Order of operations

>> TODO. ingest >> dbt >> enrich >> dbt ...

### Docker Setup

I run moomoo on my local desktop which also hosts the postgres db. I execute jobs on an airflow server hosted on the same machine, so I've dockerized an environment that works for me.

I export the following env vars:

```
DOCKER_POSTGRES_DSN="dbname=my_db host=host.docker.internal ..."
DOCKER_DBT_PG_HOST="host.docker.internal"
DOCKER_DBT_PG_PORT=5432
DOCKER_DBT_PG_USER="..."
DOCKER_DBT_PG_DBNAME="..."
DOCKER_DBT_PG_SCHEMA="..."
DOCKER_DBT_PG_PASSWORD="..."
DOCKER_MOUNT_LOCAL_MUSIC_DIR=/user/me/music  # mounted at /mnt/moomoo/music in docker
```

And

```sh
docker-compose up --build
```

Then run arbitrary whatever in that container via:

```sh
docker-compose run moomoo ...
# like docker-compose run moomoo make dbt-run
```

### Database

This postgres table stores the raw data obtained from the Last FM API. My goal is to run a script daily to append values here.

> TODO: db diagram

#### DBT

If you have data being populated into the db; you can use the models in [dbt](dbt/) to expose those json payloads into useful/structured data.
