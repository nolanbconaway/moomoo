# `moomoo/ml`: Machine Learning Models

[![ml](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml/badge.svg)](https://github.com/nolanbconaway/moomoo/actions/workflows/ml.yml)

This package contains the machine learning models to process audio data into vectors which are stored in the database. It needs to be run on a server with good access to the media library, since files are being processed.

The model in use is [`MERT-v1-330M`](https://huggingface.co/m-a-p/MERT-v1-330M). Initial embeddings from that model are also "conditioned" via a PCA transform which contextualizes the data given the available media library (which presumably reflects something about the user's taste).

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

Setup db

```
moomoo-ml create-db
```

Download the model locally:

```
moomoo-ml scorer save-artifacts
```

The run scoring via:**
**
```
moomoo-ml scorer score /path/to/media-library
```

Build the conditioning model like

```
moomoo-ml conditioner build --update-info
```

And save conditioned embeddings

```
moomoo-ml conditioner score
```

## Docker 

The [nvidia container toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/user-guide.html) is needed to run ml work in docker. Download the model locally:

```
moomoo-ml save-artifacts
```

Save the conditioning model to a pickle

```
moomoo-ml conditioner build --update-info
```

Build

```
make docker-build
```

Run

```
make docker-run-scorer
```

```
make docker-run-conditioner
```