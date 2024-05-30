FROM python:3.10-slim

ARG DAGSTER_VERSION

RUN mkdir -p /opt/dagster/dagster_home

RUN pip install \
    dagster-webserver=${DAGSTER_VERSION} \
    dagster-postgres \
    dagster-docker

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

WORKDIR /opt/dagster/dagster_home

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]