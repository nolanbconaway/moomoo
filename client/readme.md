# `moomoo/client`: Client Package

[![client](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml)

This package contains the client-centric application. It is designed to communicate with `moomoo/http` on the local machine, in order to make playlists and add them into the client music player.

Use of the client requires something to ensure the same access to the media library as on the server. This could be a SFTP mount, or running the client directly on the server.

## Install/usage

> TODO: pip installable somehow?

Point to the media library and the `moomoo/http` server with these envvars:

```sh
MOOMOO_HOST=host:port
MOOMOO_MEDIA_LIBRARY=/files/where/music
```

Create a strawberry playlist from a file

```sh
moomoo-client playlist from-path --username=... --out=strawberry /path/to/file.mp3
```

## Development

Local dev requires running a `moomoo/http` server or aggressive mocks. Perhaps later i write up a test server?
