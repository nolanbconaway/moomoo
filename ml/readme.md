# `moomoo/ml`: Machine Learning Models

[![ml](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml)

This package contains the machine learning models to process audio data into vectors which are stored in the database. It needs to be run on a server with good access to the media library, since files are being processed.

The model in use is [`MERT-v1-330M`](https://huggingface.co/m-a-p/MERT-v1-330M).

The CLI design is to run periodically, adding new embeddings into the database as they are found.

Use the following envvars:

```sh
MOOMOO_POSTGRES_URI=...
MOOMOO_ML_DEVICE=gpu # or cpu

# for docker
MOOMOO_DOCKER_POSTGRES_URI=host.docker.internal...  # or whatever
MOOMOO_MEDIA_LIBRARY=/path/to/music  # mounted at /mnt/music in docker.
```

## Usage

Download the model locally:

```
moomoo-ml save-artifacts
```

The run scoring via:

```
moomoo-ml score /path/to/media-library
```

## Docker 

The [nvidia container toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/user-guide.html) is needed to run ml work in docker. Download the model locally:

```
moomoo-ml save-artifacts
```

Build

```
make docker-build
```

Run

```
make docker-run
```