FROM nvidia/cuda:13.2.0-runtime-ubuntu24.04

RUN apt-get update && apt-get install -y \
    curl \
    make \
    git \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/nightly \
    && apt-get update && apt-get install -y \
    python3.14 \
    python3.14-dev \
    python3.14-venv \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.14

RUN ln -sf $(which python3.14) /usr/bin/python && \
    ln -sf $(which python3.14) /usr/bin/python3

RUN pip config set global.break-system-packages true

WORKDIR /usr/src/moomoo
COPY src src
COPY pyproject.toml .
RUN pip install -e .[all] --no-cache-dir
COPY Makefile .
COPY artifacts ./artifacts