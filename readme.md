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
CONTACT_EMAIL=...
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

A postgres DB is needed for basically everything. I run postgres 13 locally. [`pgvector`](https://github.com/pgvector/pgvector) is needed to support embeddings.

> TODO: db diagram

#### DBT Design

Database sources are iteratively processed, currently in this rough order:

1. Source data collected from local files and listenbrainz. No dependencies outside the python jobs.
2. *TODO: Resolution of the mbids for each local file that did not store its own mbids.*
3. A list of mbid/entity combinations built via dbt, unioning the above sources.
4. Enriched source data for each mbid from step 3, obtained in a python job by querying musicbrainz.
5. Dimensional model making the enriched data per mbid available.

