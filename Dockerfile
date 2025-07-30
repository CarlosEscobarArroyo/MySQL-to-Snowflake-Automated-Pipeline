FROM apache/airflow:2.5.1-python3.10

ENV AIRFLOW_VERSION=2.5.1
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow

RUN pip install --no-cache-dir \
    pymysql \
    pandas \
    numpy \
    openpyxl \
    snowflake-connector-python \
    python-dotenv \
    apache-airflow-providers-snowflake \
    --constraint "${CONSTRAINT_URL}"
