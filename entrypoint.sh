#!/bin/bash

echo "Waiting for MySQL to start..."
while ! nc -z mysql 3306; do
    sleep 1
done
echo "MySQL is up and running!"

if [ ! -f "/opt/airflow/airflow.db_initialized" ]; then
    echo "Initializing Airflow database..."
    airflow db migrate
    touch /opt/airflow/airflow.db_initialized
else
    echo "Airflow database already initialized."
fi

airflow users create \
  --username admin_3453511 \
  --password admin_3453511 \
  --firstname admin_3453511 \
  --lastname admin_3453511 \
  --email admin_3453511@example.com \
  --role Admin


export PYTHONNOUSERSITE=1

airflow webserver -p 8080 & airflow scheduler


