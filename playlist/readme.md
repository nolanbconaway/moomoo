# `moomoo/playlist`: Playlist generation and storage.

[![playlist](https://github.com/nolanbconaway/moomoo/actions/workflows/playlist.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/playlist.yml)

This package contains a combination of library code for creating playlists (for eventual re-use in [`../http`](../http)) and CLI handlers for saving "collections" of playlists to the database. A "collection" is multiple playlists created via the same process (e.g., playlists inspired by the user's top artists, etc.).

Originally, playlists were created on the fly in the http app, but I realized some generators ran longer than felt good within a http situation. So now the big ones are pr-saved to the database for quick retrieval.

Set these envvars for ease of use:

```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_DBT_SCHEMA=...
LISTENBRAINZ_USERNAME=...

# i also set this so because i run everything on one machine / use host.docker.internal
MOOMOO_DOCKER_POSTGRES_URI=...
```

### Usage

```sh
$ moomoo-playlist create-db
$ moomoo-playlist loved-tracks
$ moomoo-playlist revisit-releases
$ moomoo-playlist top-artists
```

### Docker

```sh
make docker-build
```

Use the `cmd=` variable to pass instructions to docker run.

```sh
make docker-run cmd=loved-tracks
```