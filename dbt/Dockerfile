FROM ghcr.io/dbt-labs/dbt-postgres:1.9.0

COPY docker-profiles.yml /root/.dbt/profiles.yml 
COPY dbt_project.yml dbt_project.yml
COPY macros macros
COPY tests tests
COPY models models
COPY packages.yml packages.yml

# make sure we update to the required dbt version. the docker container runs dbt 1.9.0 and we want 1.9.1
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# download artifacts
RUN dbt deps

ENV DBT_PRINTER_WIDTH=100
ENV DBT_USE_COLOR=false

# ENTRYPOINT ["dbt"] # this is the default