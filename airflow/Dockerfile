FROM apache/airflow:2.10.2-python3.10
COPY ./requirements.txt /requirements.txt
COPY common /opt/airflow/common
ENV PYTHONPATH="/opt/airflow/common:${PYTHONPATH}"
RUN pip install --no-cache-dir -r /requirements.txt
COPY ./dags /opt/airflow/dags