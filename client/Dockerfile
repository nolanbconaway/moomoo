FROM python:3.14-slim
RUN apt update

WORKDIR /src

# setup py modules
COPY src src
COPY setup.py .
COPY readme.md .
RUN pip install . --no-cache-dir

COPY Makefile .

# expose bash shell
CMD ["bash"]