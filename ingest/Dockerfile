FROM python:3.10-slim-bullseye
RUN apt update
RUN apt install -y make
RUN apt-get install libgomp1

WORKDIR /src

# setup py modules
COPY src src
COPY setup.py .
RUN pip install . --no-cache-dir

COPY Makefile .

# expose bash shell
CMD ["bash"]