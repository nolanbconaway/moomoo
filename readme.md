# `moomoo`: Nolan's Homemade Music Recommendation System.

![](https://archives.bulbagarden.net/media/upload/5/5f/MooMoo_Farm_anime.png)

I want to ditch Spotify and the other major streaming platforms, but I also love the recommendation/playlisting products they provide.
I'll never do it so long as I cannot get that service elsewhere, so let's see if I can't use open source software to get what I need.

Moomoo is very much an ongoing effort. Nobody else should even read this, let alone try to deploy it themselves.

## CI Status

- [![ingest](https://github.com/nolanbconaway/moomoo/actions/workflows/ingest.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/ingest.yml)
- [![ml](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml)
- [![dbt](https://github.com/nolanbconaway/moomoo/actions/workflows/dbt.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/dbt.yml)
- [![client](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml)
- [![playlist](https://github.com/nolanbconaway/moomoo/actions/workflows/playlist.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/playlist.yml)
- [![http](https://github.com/nolanbconaway/moomoo/actions/workflows/http.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/http.yml)
  
## Architecture

Moomoo is composed of (currently) 6 components that work together through the use of a central postgres database. [`pgvector`](https://github.com/pgvector/pgvector) is needed in that database to store and manage ML embeddings.

The general setup is:

- [`ml`](ml/), [`ingest`](ingest/) populate base tables in postgres (with some exception in `ingest`). These modules are run via a scheduler like airflow, etc.
- [`dbt`](dbt/) merges tables and populates tested/consumable data. It also populates a main list of [mbids](https://musicbrainz.org/doc/MusicBrainz_Identifier) which are consumed by some `ingest` jobs.
- [`playlist`](playlist/) contains a combination of library code for creating playlists (for re-use in [`http`](http/)) and CLI handlers for saving collections of playlists to the database.
- [`http`](http/) provides a webserver through which playlists are requested. Database access from the client is managed exclusively through the `http` api.
- [`client`](client/) provides an installable (via pipx, etc) package for local clients. Its requirements are minimal and should be lightweight. It currently only contains a CLI to generate playlists, but will likely evolve into a larger GUI application.

Each component requires some special envvars, etc. So see the docs within the folder for each. I have no advice on how to orchestrate these services.
