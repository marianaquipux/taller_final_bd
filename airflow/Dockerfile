FROM apache/airflow:2.0.2

USER airflow

# Instalar librerías adicionales
RUN python -m pip install --upgrade pip
RUN pip install confluent_kafka mysql-connector-python kafka-python pandas psycopg2-binary apache-airflow-providers-postgres pymongo sqlalchemy