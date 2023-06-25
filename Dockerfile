FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

RUN apt update
RUN apt install -y make
RUN apt-get install -y python3.10 python3-pip

# make python3.10 into python
RUN ln -s $(which python3.10) /usr/bin/python

WORKDIR /usr/src/moomoo

# setup py modules
COPY src/moomoo ./src/moomoo
COPY setup.py .
RUN pip install pip==23.* --no-cache-dir
RUN pip install -e .[dbt,test] --no-cache-dir

# setup dbt
COPY dbt/docker-profiles.yml /root/.dbt/profiles.yml 
COPY dbt/dbt_project.yml ./dbt/dbt_project.yml
COPY dbt/macros ./dbt/macros
COPY dbt/models ./dbt/models
COPY dbt/packages.yml ./dbt/packages.yml

COPY Makefile .

# download artifacts
RUN make dbt-deps
RUN moomoo ml save-artifacts