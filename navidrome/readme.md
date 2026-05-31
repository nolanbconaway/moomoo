# `moomoo/navidrome`: Navidrome Integration

[![navidrome](https://github.com/nolanbconaway/moomoo/actions/workflows/navidrome.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/navidrome.yml)

This package provides bidirectional sync between Navidrome and ListenBrainz, as well as playlist management from moomoo collections. It integrates with Navidrome via both the Subsonic API and direct SQLite database access.

The CLI is designed to be run periodically, as in an airflow server or similar.

## Features

- **Playlist Sync**: Sync moomoo playlist collections to Navidrome
- **Love Sync**: Bidirectional sync of loved/favorited tracks between ListenBrainz and Navidrome  
- **Play Queue**: Manage an auto-updating play queue playlist with recently added media

Use the following envvars:

```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_DBT_SCHEMA=...  # schema target from dbt. needed for some jobs.

NAVIDROME_URL=...  # e.g., http://localhost:4533
NAVIDROME_USERNAME=...
NAVIDROME_PASSWORD=...
NAVIDROME_DB_PATH=...  # path to navidrome.db. needed for media file resolution.

LISTENBRAINZ_USERNAME=...
LISTENBRAINZ_USER_TOKEN=...

MOOMOO_DOCKER_POSTGRES_URI=host.docker.internal... # or whatever, for docker usage
```

## Usage

Some example use of each CLI command:

```sh
# Sync playlists from moomoo collections to Navidrome
$ moomoo-navidrome playlist sync
$ moomoo-navidrome playlist sync --force  # force re-sync even if up to date

# Sync loved tracks between ListenBrainz and Navidrome
$ moomoo-navidrome loves sync
$ moomoo-navidrome loves sync --direction=navidrome-to-listenbrainz
$ moomoo-navidrome loves sync --direction=listenbrainz-to-navidrome

# Manage the play queue playlist
$ moomoo-navidrome play-queue sync
```

### Docker

```sh
make docker-build
```

Use the `cmd=` variable to pass instructions to docker run.

```sh
make docker-run cmd='playlist sync --force'
make docker-run cmd='loves sync'
make docker-run cmd='play-queue sync'
```

## Development

```sh
# Install dependencies
pip install -e .[test]

# Run tests
make test

# Format and lint
make format
make lint
```
