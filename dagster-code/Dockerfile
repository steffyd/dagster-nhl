FROM python:3.10-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load
# the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-airbyte \
    dagster-docker \
    dagster-slack \
    sqlalchemy==1.4.46

RUN pip install \
    pyodbc \
    requests \
    xgboost \
    pandas \
    scikit-learn

RUN apt update && \
    apt install -y gnupg curl && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

# Set $DAGSTER_HOME and copy dagster instance there

ENV DAGSTER_HOME=/opt/dagster/dagster_home

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