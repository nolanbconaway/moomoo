FROM nvidia/cuda:13.3.0-runtime-ubuntu26.04

RUN apt-get update && apt-get install -y \
    curl \
    make \
    git \
    python3.14 \
    python3.14-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf $(which python3.14) /usr/bin/python && \
    ln -sf $(which python3.14) /usr/bin/python3
RUN pip config set global.break-system-packages true

WORKDIR /usr/src/moomoo

# setup py modules
COPY src src
COPY pyproject.toml .
RUN pip install -e .[all] --no-cache-dir

COPY Makefile .

# if not here, run moomoo ml save-artifacts
COPY artifacts ./artifacts
