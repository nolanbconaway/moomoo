FROM python:3.14-slim-bookworm
RUN apt update
RUN apt install -y make
RUN apt-get install -y libgomp1 git

WORKDIR /src

# setup py modules
COPY src src
COPY pyproject.toml .
RUN pip install . --no-cache-dir

COPY Makefile .

# expose bash shell
CMD ["bash"]