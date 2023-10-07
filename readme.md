# `moomoo`: Nolan's Homemade Music Recommendation System.

[![Unit Tests](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/push.yml)

![](https://archives.bulbagarden.net/media/upload/5/5f/MooMoo_Farm_anime.png)

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use open source software to get what I need.

## Status

This is ongoing! 

Current status: I am using this locally in place of spotify. I need to collect more data on playlists to improve the ranking algorithm/ml model.

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

```sh
# general local dev
MOOMOO_POSTGRES_URI=... # for local dev
MOOMOO_CONTACT_EMAIL=...
MOOMOO_INGEST_SCHEMA='public'
MOOMOO_DBT_SCHEMA='moomoo_dbt'

MOOMOO_ML_DEVICE=cuda # or cpu (optional)

# for docker only
MOOMOO_DOCKER_POSTGRES_URI=host=host.docker.internal...
MOOMOO_MOUNT_LOCAL_MUSIC_DIR=...

# for dbt; if templating profiles.yml
MOOMOO_DBT_PG_HOST=...
MOOMOO_DBT_PG_PORT=...
MOOMOO_DBT_PG_USER=...
MOOMOO_DBT_PG_PASSWORD=...
MOOMOO_DBT_PG_DBNAME=...

# client application for accessing http
MOOMOO_HOST=host:port
MOOMOO_MEDIA_LIBRARY=/files/where/music
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

Local development requires a valid dbt profiles.yml, and these envvars

```
MOOMOO_POSTGRES_URI=...
MOOMOO_INGEST_SCHEMA=...
MOOMOO_DBT_SCHEMA=...
MOOMOO_CONTACT_EMAIL=...
```

Docker will require These envvars:

```sh
MOOMOO_DOCKER_POSTGRES_URI=host=host.docker.internal...
MOOMOO_INGEST_SCHEMA='public'
MOOMOO_DBT_SCHEMA='moomoo_dbt'
MOOMOO_CONTACT_EMAIL=...
MOOMOO_MOUNT_LOCAL_MUSIC_DIR=...

# for dbt
MOOMOO_DBT_PG_HOST=...
MOOMOO_DBT_PG_PORT=...
MOOMOO_DBT_PG_USER=...
MOOMOO_DBT_PG_PASSWORD=...
MOOMOO_DBT_PG_DBNAME=...
```

Client installations need to point to the `MOOMOO_HOST`, and to the local `MOOMOO_MEDIA_LIBRARY`.

### HTTP Server

The HTTP server manages access from the client to the database built above, and makes playlists, etc. Envvars:

```
MOOMOO_POSTGRES_URI=...
MOOMOO_INGEST_SCHEMA=...
MOOMOO_DBT_SCHEMA=...
MOOMOO_CONTACT_EMAIL=...
```

See the `make docker-http-serve` target for an easy way to run this.

### Client CLI

This part is undergoing the most iteration. The CLI is intended to provide an easy way to make playlists from the server, in a way that can be consumed by the client.

Client will need to SFTP mount the server media directory to ensure the same access to the media library (or be ran directly on the host), and export envvars pointing to the server:

```
MOOMOO_HOST=localhost:5600 # etc
MOOMOO_MEDIA_LIBRARY=/mount/server/media
```

For integrations with media players (strawberry, etc), install the client directly via pipx or a venv. Then a CLI invocation can be wrapped in a small script:

```sh
moomoo client playlist from-path ~/Music/Artist/Album --out=strawberry
```


