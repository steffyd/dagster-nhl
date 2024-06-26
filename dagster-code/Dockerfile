FROM python:3.10-slim

ARG DAGSTER_VERSION

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load
# the DagsterInstance

RUN pip install \
    numpy==1.26.4 \
    dagster==${DAGSTER_VERSION} \
    dagster-postgres \
    dagster-dbt \
    dagster-docker \
    dagster-slack \
    dagster-gcp \
    dagster-gcp-pandas \
    bigquery \
    sqlalchemy==1.4.46 \
    pyodbc \
    requests \
    xgboost \
    pandas \
    scikit-learn \
    dbt-bigquery \

RUN apt-get update && apt-get install -y git openssh-client

# Set $DAGSTER_HOME and copy dagster instance there

ENV DAGSTER_HOME=/opt/dagster/app

RUN mkdir -p $DAGSTER_HOME

#COPY dagster.yaml $DAGSTER_HOME

# Add repository code

WORKDIR /opt/dagster/app

#COPY repo.py /opt/dagster/app

# Run dagster gRPC server on port 4000

EXPOSE 4000

# Using CMD rather than ENTRYPOINT allows the command to be overridden in
# run launchers or executors to run other commands using this image
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repo.py", "--working-directory", "/opt/dagster/app/"]
#CMD ["tail", "-f", "/dev/null"]