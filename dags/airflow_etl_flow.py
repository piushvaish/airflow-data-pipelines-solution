from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helpers.sql_queries import SqlQueries
from helpers.create_tables import CreateTables
from create_table_subdag import create_table_dag
start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'depends_on_past':False,
    'retries': 3,
    'catchup':False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5), 
    'start_date':datetime(2020, 1, 4),
    'end_date':datetime(2020, 5, 4)
}

dag = DAG('airflow_etl_flow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Creating Staging tables
create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_staging_events
)
    
create_songs_table = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_staging_songs
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json="auto"
)

# Create fact table : songplays
create_songplays = PostgresOperator(
    task_id="create_songplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.create_songplays
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    append_only=False,
    query=SqlQueries.songplay_table_insert
)

# users
load_user_dimension_table = SubDagOperator(
    subdag=create_table_dag(
        parent_dag_name='airflow_etl_flow',
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        create_sql_stmt=CreateTables.create_users,
        insert_sql_stmt=SqlQueries.user_table_insert,        
        table_exists=True,
        start_date=start_date

    ),
    task_id='Load_user_dim_table',
    dag=dag,
)

# song
load_song_dimension_table = SubDagOperator(
    subdag=create_table_dag(
        parent_dag_name='airflow_etl_flow',
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        create_sql_stmt=CreateTables.create_songs,
        insert_sql_stmt=SqlQueries.song_table_insert,        
        table_exists=True,
        start_date=start_date

    ),
    task_id='Load_song_dim_table',
    dag=dag,
)

# artist
load_artist_dimension_table = SubDagOperator(
    subdag=create_table_dag(
        parent_dag_name='airflow_etl_flow',
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        create_sql_stmt=CreateTables.create_artists,
        insert_sql_stmt=SqlQueries.artist_table_insert,        
        table_exists=True,
        start_date=start_date

    ),
    task_id='Load_artist_dim_table',
    dag=dag,
)

# time
load_time_dimension_table = SubDagOperator(
    subdag=create_table_dag(
        parent_dag_name='airflow_etl_flow',
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        create_sql_stmt=CreateTables.create_time,
        insert_sql_stmt=SqlQueries.time_table_insert,        
        table_exists=True,
        start_date=start_date

    ),
    task_id='Load_time_dim_table',
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.songplays", "public.users", "public.songs", "public.artists", "public.time"]
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task Dependencies
start_operator >> create_staging_events >> stage_events_to_redshift
start_operator >> create_songs_table >> stage_songs_to_redshift

stage_events_to_redshift >> create_songplays
stage_songs_to_redshift >> create_songplays

create_songplays >> load_songplays_table

load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator