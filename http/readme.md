# `moomoo/http`: HTTP Server for Moomoo Playlists

[![http](https://github.com/nolanbconaway/moomoo/actions/workflows/http.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/http.yml)

This package contains a HTTP server which reads data managed by `moomoo/dbt` to serve requests from `moomoo/client`.

This can be run theoretically anywhere, so long as it is pointed correctly at the postgres database managed by dbt.

Set these envvars to do so:


```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_DBT_SCHEMA=...

# i also set this so because i run everything on one machine / use host.docker.internal
MOOMOO_DOCKER_POSTGRES_URI=...
```

## Usage

> TODO python install instructions.

This CLI will serve the application.

```sh
moomoo-http serve
```

### Docker

```sh
make docker-build
```

and 

```sh
make docker-http-serve
```
