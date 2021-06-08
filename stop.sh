cd airflow
rm *.err *.log *.out *.pid
#airflow.db
rm -r logs

PIDS=$(ps aux | grep airflow | awk '{print $2}')

echo "$PIDS"

kill -9 $PIDS
