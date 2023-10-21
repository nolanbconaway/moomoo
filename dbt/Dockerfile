FROM ghcr.io/dbt-labs/dbt-postgres:1.6.3

COPY docker-profiles.yml /root/.dbt/profiles.yml 
COPY dbt_project.yml dbt_project.yml
COPY macros macros
COPY tests tests
COPY models models
COPY packages.yml packages.yml

# download artifacts
RUN dbt deps
# ENTRYPOINT ["dbt"] # this is the default