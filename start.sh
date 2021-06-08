#!/bin/bash

source .venv/bin/activate

export AIRFLOW_HOME="$PWD/airflow"

#;alias airflow=".venv/lib/python3.8/site-packages/airflow/bin/airflow"

# Start airflow
airflow db init
airflow scheduler --daemon
airflow webserver --daemon -p 3000

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done