import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers.udacity.warmup import read_states, read_mode, read_port, \
    read_visa, read_demographics, read_country
from operators.udacity import StageToS3, DataQualityOperator

"""
CONSTANTS
"""
DEFAULT_ARGS = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(
        minutes=5
    ),
    'catchup': False
}
BASE_PATH: str = os.path.abspath(
    os.path.join(
        os.path.dirname(
            __file__
        ),
        '..',
        '..'
    ),
)

AWS_CONN_ID: str = "aws"
REDSHIFT_CONN_ID: str = "redshift"
S3_BUCKET_NAME: str = "datalakebucketjsb/udacity/capstone"
S3_STATES: str = f'{S3_BUCKET_NAME}/states/states.parquet'
S3_COUNTRIES: str = f"{S3_BUCKET_NAME}/countries/countries.parquet"
S3_MODES: str = f"{S3_BUCKET_NAME}/modes/modes.parquet"
S3_PORTS: str = f"{S3_BUCKET_NAME}/ports/ports.parquet"
S3_VISAS: str = f"{S3_BUCKET_NAME}/visas/visas.parquet"
S3_DEMOGRAPHICS: str = f"{S3_BUCKET_NAME}/demographics/demographics.parquet"
S3_IMMIGRATIONS: str = f"{S3_BUCKET_NAME}/immigration/immigration.parquet"

TESTS: list = [
    'records.content_length > 0',
]

"""
AIRFLOW API
"""

dag = DAG(
    'capstone_project',
    default_args=DEFAULT_ARGS,
    description='Load from local into S3',
    schedule_interval='@once'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_states_to_s3 = StageToS3(
    bucket=S3_STATES,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='I94ADDR.csv',
    read_func=read_states,
    task_id='Stage_States',
    dag=dag
)

stage_countries_to_s3 = StageToS3(
    bucket=S3_COUNTRIES,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='I94CIT_I94RES.csv',
    read_func=read_country,
    task_id='Stage_Countries',
    dag=dag
)

stage_modes_to_s3 = StageToS3(
    bucket=S3_MODES,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='I94MODE.csv',
    read_func=read_mode,
    task_id='Stage_Modes',
    dag=dag
)

stage_ports_to_s3 = StageToS3(
    bucket=S3_PORTS,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='I94PORT.csv',
    read_func=read_port,
    task_id='Stage_Port',
    dag=dag
)

stage_visa_to_s3 = StageToS3(
    bucket=S3_VISAS,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='I94VISA.csv',
    read_func=read_visa,
    task_id='Stage_Visa',
    dag=dag
)

stage_demographics_to_s3 = StageToS3(
    bucket=S3_DEMOGRAPHICS,
    partition_by=None,
    s3_conn_id=AWS_CONN_ID,
    path='us-cities-demographics.csv',
    read_func=read_demographics,
    task_id='Stage_demographics',
    dag=dag
)

run_stagging_checks = DataQualityOperator(
    tests={
        S3_STATES: TESTS,
        S3_COUNTRIES: TESTS,
        S3_MODES: TESTS,
        S3_PORTS: TESTS,
        S3_VISAS: TESTS,
        S3_DEMOGRAPHICS: TESTS,
    },
    bucket=S3_BUCKET_NAME,
    s3_conn_id=AWS_CONN_ID,
    task_id='Run_data_quality_checks',
    dag=dag
)

spark_job = SparkSubmitOperator(
    task_id='Immigration_spark_job',
    conn_id='spark',
    application=os.path.join(
        BASE_PATH,
        'immigration_job.py'
    ),
    files=",".join(
        [
            os.path.join(
                BASE_PATH,
                'dl.cfg'
            ),
            os.path.join(
                BASE_PATH,
                'data',
                'immigration_data_sample.csv'
            )
        ]
    ),
    packages="com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.0",
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

run_spark_checks = DataQualityOperator(
    tests={
        S3_IMMIGRATIONS: TESTS,
    },
    bucket=S3_BUCKET_NAME,
    s3_conn_id=AWS_CONN_ID,
    task_id='Run_spark_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> [
    stage_states_to_s3,
    stage_countries_to_s3,
    stage_modes_to_s3,
    stage_ports_to_s3,
    stage_visa_to_s3,
    stage_demographics_to_s3
] >> run_stagging_checks >> spark_job >> run_spark_checks >> end_operator
