FROM apache/airflow:2.10.2-python3.10
COPY ./requirements.txt /requirements.txt


USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER airflow
COPY common /opt/airflow/common
ENV PYTHONPATH="/opt/airflow/common:${PYTHONPATH}"
RUN pip install --no-cache-dir -r /requirements.txt
COPY ./dags /opt/airflow/dags