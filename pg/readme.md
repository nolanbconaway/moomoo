# `moomoo/pg`: SQLAlchemy table definitions and database utilities.

[![pg](https://github.com/nolanbconaway/moomoo/actions/workflows/pg.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/pg.yml)

This package contains shared SQLAlchemy ORM definitions and database utility functions used across the moomoo subprojects.

Set these envvars for ease of use:

```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_DOCKER_POSTGRES_URI=host.docker.internal... # or whatever
```

## Usage 

Some example use of the CLI:

```sh
$ moomoo-pg ddl --all
$ moomoo-pg ddl local_music_files
$ moomoo-pg create local_music_files
```

### Docker

```sh
make docker-build
```

Use `make docker-run` to pass commands into the container:

```sh
make docker-run cmd='ddl local_music_files'
```