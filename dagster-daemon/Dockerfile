FROM python:3.10-slim

ARG DAGSTER_VERSION
# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load
# the DagsterInstance

RUN pip install \
    dagster==${DAGSTER_VERSION} \
    dagster-postgres \
    dagster-docker \
    sqlalchemy==1.4.46

# Set $DAGSTER_HOME and copy dagster instance there

ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

#COPY dagster.yaml workspace.yaml $DAGSTER_HOME

# Add repository code

WORKDIR $DAGSTER_HOME

#COPY repo.py /opt/dagster/app

# Run dagster gRPC server on port 4000

EXPOSE 4000

# Using CMD rather than ENTRYPOINT allows the command to be overridden in
# run launchers or executors to run other commands using this image
CMD ["dagster-daemon", "run"]