# `moomoo/client`: Client Package

[![client](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/client.yml)

This package contains the client-centric application. It is designed to communicate with `moomoo/http` on the local machine, in order to make playlists and add them into the client music player.

Use of the client requires something to ensure the same access to the media library as on the server. This could be a SFTP mount, or running the client directly on the server.

## Install

This is intended to be a lightweight pypackage, installed via [`pipx`](https://github.com/pypa/pipx) or similar.

Install from git via:

```sh
pipx install git+https://github.com/nolanbconaway/moomoo.git#subdirectory=client
```

> NOTE: Only python 3.9 and 3.10 are tested, so use the `--python=/path/to/py3.9/bin/python` pipx option if needed.

## Usage

Point to the media library and the `moomoo/http` server with these envvars:

```sh
MOOMOO_HOST=host:port
MOOMOO_MEDIA_LIBRARY=/files/where/music
LISTENBRAINZ_USERNAME=...
```

Create a [strawberry](https://www.strawberrymusicplayer.org/) playlist from a file

```sh
moomoo-client playlist from-path --username=... --out=strawberry /path/to/file.mp3
```

Use the new suggested playlists features:

```sh
moomoo-client playlist suggest-artists
```

### :warning: Experimental GUI

I'm not good at GUI programming. I wrote it in Toga but I have no idea if that was a good choice for this.

```sh
moomoo-client gui
```

## Development

Local dev requires running a `moomoo/http` server or aggressive mocks. Perhaps later i write up a test server?
