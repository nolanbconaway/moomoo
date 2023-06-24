FROM python:3.10-slim-buster

RUN apt update && apt install -y make
RUN pip install --upgrade pip

WORKDIR /usr/src/moomoo

# setup py modules
COPY src/moomoo ./src/moomoo
COPY setup.py .
RUN pip install pip==23.*
RUN pip install -e .[dbt,test]

# setup dbt
COPY dbt/docker-profiles.yml /root/.dbt/profiles.yml 
COPY dbt/dbt_project.yml ./dbt/dbt_project.yml
COPY dbt/macros ./dbt/macros
COPY dbt/models ./dbt/models
COPY dbt/packages.yml ./dbt/packages.yml

COPY Makefile .
RUN make dbt-deps
