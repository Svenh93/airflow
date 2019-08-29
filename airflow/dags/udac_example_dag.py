from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key

default_args = {
    'owner': 'udacity',
    'depends_on_past' : False,
    'start_date': datetime(2018, 11, 1),
    'end_date' : datetime(2018, 11, 2),
    'retries' : 2,
    'retry_delay' : timedelta(minutes = 1),
    'catchup' : False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name = 'stage_event',
    redshift_conn_id    = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data/',
    delimiter = ',',
    headers = 1,
    quote_char = '"',
    file_type = 'json',
    aws_credentials = {
            'key': AWS_KEY,
            'secret': AWS_SECRET},
    region = 'us-west-2', 
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name ='stage_song',
    redshift_conn_id = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/',
    delimiter = ',',
    headers = 1,
    quote_char = '"',
    file_type = 'json',
    aws_credentials = {
            'key': AWS_KEY,
            'secret': AWS_SECRET},
    region = 'us-west-2', 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    truncate = '',
    table_name = 'songplay',
    table_name_trunc = '',
    redshift_conn_id = 'redshift',
    sql_statement = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name = 'users',
    redshift_conn_id = 'redshift',
    table_name_trunc = 'users',
    truncate = 'truncate',
    sql_statement = SqlQueries.user_table_insert   
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name = 'song',
    redshift_conn_id = 'redshift',
    table_name_trunc = 'song',
    truncate = 'truncate',
    sql_statement = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name = 'artist',
    redshift_conn_id = 'redshift',
    table_name_trunc = 'artist',
    truncate = 'truncate',
    sql_statement = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name = 'time',
    redshift_conn_id = 'redshift',
    table_name_trunc = 'time',
    truncate = 'truncate',
    sql_statement = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_names = ['users', 'time', 'song', 'artist', 'songplay']
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

start_operator >> drop_tables >> create_tables >> end_operator

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator