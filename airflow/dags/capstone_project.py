from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from helpers.udacity.warmup import read_addr, read_mode, read_port, read_visa, \
    read_demographics, read_country
from operators.udacity import StageToS3, DataQualityOperator

from airflow import DAG

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

AWS_CONN_ID: str = "aws"
REDSHIFT_CONN_ID: str = "redshift"
S3_BUCKET_NAME: str = "datalakebucketjsb/udacity/capstone"
S3_ADDRESSES: str = f'{S3_BUCKET_NAME}/addresses/addresses.parquet'
S3_COUNTRIES: str = f"{S3_BUCKET_NAME}/countries/countries.parquet"
S3_MODES: str = f"{S3_BUCKET_NAME}/modes/modes.parquet"
S3_PORTS: str = f"{S3_BUCKET_NAME}/ports/ports.parquet"
S3_VISAS: str = f"{S3_BUCKET_NAME}/visas/visas.parquet"
S3_DEMOGRAPHICS: str = f"{S3_BUCKET_NAME}/demographics/demographics.parquet"

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

stage_addresses_to_s3 = StageToS3(
    bucket=S3_ADDRESSES,
    partition_by=None,
    s3_conn_id='aws',
    path='I94ADDR.csv',
    read_func=read_addr,
    task_id='Stage_Addresses',
    dag=dag
)

stage_countries_to_s3 = StageToS3(
    bucket=S3_COUNTRIES,
    partition_by=None,
    s3_conn_id='aws',
    path='I94CIT_I94RES.csv',
    read_func=read_country,
    task_id='Stage_Countries',
    dag=dag
)

stage_modes_to_s3 = StageToS3(
    bucket=S3_MODES,
    partition_by=None,
    s3_conn_id='aws',
    path='I94MODE.csv',
    read_func=read_mode,
    task_id='Stage_Modes',
    dag=dag
)

stage_ports_to_s3 = StageToS3(
    bucket=S3_PORTS,
    partition_by=None,
    s3_conn_id='aws',
    path='I94PORT.csv',
    read_func=read_port,
    task_id='Stage_Port',
    dag=dag
)

stage_visa_to_s3 = StageToS3(
    bucket=S3_VISAS,
    partition_by=None,
    s3_conn_id='aws',
    path='I94VISA.csv',
    read_func=read_visa,
    task_id='Stage_Visa',
    dag=dag
)

stage_demographics_to_s3 = StageToS3(
    bucket=S3_DEMOGRAPHICS,
    partition_by=None,
    s3_conn_id='aws',
    path='us-cities-demographics.csv',
    read_func=read_demographics,
    task_id='Stage_demographics',
    dag=dag
)


run_stagging_checks = DataQualityOperator(
    tests={
        S3_ADDRESSES: TESTS,
        S3_COUNTRIES: TESTS,
        S3_MODES: TESTS,
        S3_PORTS: TESTS,
        S3_VISAS: TESTS,
        S3_DEMOGRAPHICS: TESTS,
    },
    bucket=S3_BUCKET_NAME,
    s3_conn_id='aws',
    task_id='Run_data_quality_checks',
    dag=dag
)
#
# stage_songs_to_redshift = StageToRedshiftOperator(
#     bucket=f"{S3_BUCKET_NAME}/song-data",
#     s3_conn_id=AWS_CONN_ID,
#     redshift_table=STAGING_SONGS_TABLES,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     format='auto',
#     region='us-west-2',
#     task_id='Stage_songs',
#     dag=dag
# )
#
# load_songplays_table = LoadFactOperator(
#     redshift_table=FACT_TABLE,
#     query=SQLQueries.songplay_table_insert,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )
#
# load_user_dimension_table = LoadDimensionOperator(
#     redshift_table=USERS_TABLE,
#     query=SQLQueries.user_table_insert,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     append_data=APPEND_MODE,
#     task_id='Load_user_dim_table',
#     dag=dag
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     redshift_table=SONGS_TABLE,
#     query=SQLQueries.song_table_insert,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     append_data=APPEND_MODE,
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     redshift_table=ARTIST_TABLE,
#     query=SQLQueries.artist_table_insert,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     append_data=APPEND_MODE,
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     redshift_table=TIME_TABLE,
#     query=SQLQueries.time_table_insert,
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     append_data=APPEND_MODE,
#     task_id='Load_time_dim_table',
#     dag=dag
# )
#

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> [
    stage_addresses_to_s3,
    stage_countries_to_s3,
    stage_modes_to_s3,
    stage_ports_to_s3,
    stage_visa_to_s3,
    stage_demographics_to_s3
] >> run_stagging_checks >> end_operator
