FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

RUN apt update
RUN apt install -y make
RUN apt-get install -y python3.14 python3-pip

# make python3.14 into python
RUN ln -s $(which python3.14) /usr/bin/python
RUN pip install pip==23.* --no-cache-dir

WORKDIR /usr/src/moomoo

# setup py modules
COPY src src
COPY setup.py .
RUN pip install -e .[all] --no-cache-dir

COPY Makefile .

# if not here, run moomoo ml save-artifacts
COPY artifacts ./artifacts
