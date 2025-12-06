# `moomoo/ingest`: ETL Code

[![ingest](https://github.com/nolanbconaway/moomoo/actions/workflows/ingest.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/ingest.yml)

This package contains various data ingestion scripts, ranging from ListenBrainz data dumps to local file metadata ingest, to MusicBrainz data enrichment. It is a catch-all and has wider ranging dependencies; though nothing as heavy as ML-related deps.
It needs to be run on a server with good access to the media library, since files are being processed.

The CLI is designed to be run periodically, as in an airflow server of similar.

Use the following envvars:

```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_DBT_SCHEMA=...  # schema target from dbt. needed for some jobs.
MOOMOO_CONTACT_EMAIL=...

MOOMOO_DOCKER_POSTGRES_URI=host.docker.internal... # or whatever
MOOMOO_MEDIA_LIBRARY=/path/to/music  # mounted at /mnt/music in docker.


METABRAINZ_LIVE_DATA_TOKEN=... # to consume the live data feed (https://metabrainz.org/api/)
```

## Usage

Some example use of each CLI here:

```sh
$ moomoo-ingest db create
$ moomoo-ingest annotate-mbids --before=2023-10-20 --limit 20
$ moomoo-ingest artist-stats --before=2023-10-20 --limit 20
$ moomoo-ingest listens --since-last <username>
$ moomoo-ingest local-files ~/Music --procs=10
$ moomoo-ingest similar-user-activity <username>
```

### Docker

```sh
make docker-build
```

Use the `cmd=` variable to pass instructions to docker run.

```sh
make docker-run cmd='artist-stats --before=2023-10-20 --limit 20'

# use the mounted dir for local music in docker
make docker-run cmd='local-files /mnt/music --procs=5'
```