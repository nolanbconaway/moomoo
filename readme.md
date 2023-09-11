# `moomoo`: Nolan's Homemade Music Recommendation System.

[![Unit Tests](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml)

![](https://archives.bulbagarden.net/media/upload/5/5f/MooMoo_Farm_anime.png)

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use open source software to get what I need.

## Status

This is ongoing! 

Current status: Writing ingestion pipeline and DBT models to curate a detailed dataset on my listening behavior.

## Setup

`moomoo` is orchestrated in an (overly?) complex manner. I built it in a monorepo for some reason. The upside of this monorepo is that you theoretically only need one docker image to do it all. A postgres DB is also needed for basically everything, and [`pgvector`](https://github.com/pgvector/pgvector) is needed in that database.

Build that docker image via.

```
make docker-build
```

There are three basic components to set up, outlined below:

1. Scheduled jobs
2. HTTP server
3. Client CLI

Running it all in one go requires some management of env secrets. I have a .env like:

```
POSTGRES_DSN=... # for local dev
CONTACT_EMAIL=...

# passed to docker
DOCKER_POSTGRES_DSN=host=host.docker.internal...
DOCKER_DBT_PG_HOST=...
DOCKER_DBT_PG_PORT=...
DOCKER_DBT_PG_USER=...
DOCKER_DBT_PG_PASSWORD=...
DOCKER_DBT_PG_DBNAME=...
DOCKER_DBT_PG_SCHEMA=... # re-used as MOOMOO_DBT_SCHEMA
DOCKER_MOUNT_LOCAL_MUSIC_DIR=...

# http client
MOOMOO_HOST=...
MOOMOO_MEDIA_LIBRARY=...
```

### Scheduled jobs

The basic building blocks are scheduled batch tasks (via airflow, cron, or otherwise) consisting of:

1. Basic **ingestion** of data, running on a schedule via airflow, cron, or otherwise.
    - `moomoo ingest files` populates a table about local music files.
    - `moomoo ingest listens` populates a table about user listening behavior, via ListenBrainz.
    - `moomoo ml score` populates a table of ML embeddings for each local music file. The [nvidia container toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/user-guide.html) is needed to run ml work in docker.


2. **Merging** of the MusicBrainz `mbids` learned about above, via dbt. The models in `dbt/models/flat` are all that are needed.
    - So, `make dbt-build select=flat` is the command.

3. **Enrichment** of the mbids.
    - `moomoo enrich annotate` populates a table mapping `mbids` to json data via MusicBrainz API.
    - `moomoo enrich artist-stats` populates a table with artist statistics for each mbid.

4. **Analytic** models via dbt. This is a one shot building the rest of the models in dbt. The make command is `make dbt-build select=dim analytic`.

> TODO: db diagram

The following envvars are needed for these:

```
POSTGRES_DSN=...
CONTACT_EMAIL=...
DBT_PG_HOST=...
DBT_PG_PORT=...
DBT_PG_USER=...
DBT_PG_DBNAME=...
DBT_PG_SCHEMA=...
DBT_PG_PASSWORD=...
```

Local development requires a valid dbt profiles.yml, and:

```
POSTGRES_DSN=...
CONTACT_EMAIL=...
```

### HTTP Server

The HTTP server manages access from the client to the database built above. It runs in a separate process and needs to be updated with updates to the scheduled work. That server needs to know where the dbt models are built, because it uses those to make playlists, etc.

Envvars:

```
POSTGRES_DSN=...
MOOMOO_DBT_SCHEMA=...
```

See the `make docker-http-serve` target for an easy way to run this.

### Client CLI

This part is undergoing the most iteration. The CLI is intended to provide an easy way to make playlists from the server, in a way that can be consumed by the client.

Client will need to SFTP mount the server media directory to ensure the same access to the media library (or be ran directly on the host), and exoprt envvars pointing to the server:

```
MOOMOO_HOST=localhost:5600 # etc
MOOMOO_MEDIA_LIBRARY=/mount/server/media
```

Fo integrations with media players (strawberry, etc), install the client directly via pipx or a venv. Then a CLI invocation can be wrapped in a small script:

```
moomoo client playlist from-path ~/Music/Artist/Album --out=strawberry
```


