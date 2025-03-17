FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-kafka \
    psycopg2-binary \
    kafka-python


