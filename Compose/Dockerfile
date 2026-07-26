FROM apache/airflow:3.2.1

COPY requirements-qa.txt /opt/airflow/requirements-qa.txt

RUN pip install --no-cache-dir --requirement /opt/airflow/requirements-qa.txt
