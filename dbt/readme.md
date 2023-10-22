# `moomoo/dbt`: Data warehouse tranformations for client consumption.

[![DBT](https://github.com/nolanbconaway/moomoo/actions/workflows/dbt.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/dbt.yml)

`moomoo` leans heavily on a postgres database (with [`pgvector`](https://github.com/pgvector/pgvector) installed in that database). This package manages client-facing data models (e.g., consumed by `moomoo/http`).

Local dev requires a valid dbt `profiles.yml` installed, as well as [`yq`](https://github.com/mikefarah/yq) to parse the version tag from the dbt project.

The Docker container relies on a few exported envvars:

```
DBT_HOST=...  # i use host.docker.internal
DBT_PORT=...
DBT_USER=...
DBT_PASSWORD=...
DBT_DBNAME=...
DBT_SCHEMA=...
```

Build the container like.

```
make docker-build
```

With those envvars available, run an arbitrary dbt command like:

```sh
make docker-run cmd='build'
```