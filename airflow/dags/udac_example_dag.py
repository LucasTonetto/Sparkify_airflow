from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data/',
    region='us-west-2',
    table='staging_events',
    json_format='s3://udacity-dend/log_json_path.json'
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data/',
    region='us-west-2',
    table='staging_songs',
    json_format='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songplays',
    insert_stmt='''
      INSERT INTO songplays ( 
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id,
        session_id, 
        location, 
        user_agent 
      ) 
      SELECT
        e.start_time, 
        e.userid, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.sessionid, 
        e.location, 
        e.useragent
      FROM (
        SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong'
      ) e
      LEFT JOIN staging_songs AS s
      ON e.song = s.title
        AND e.artist = s.artist_name
        AND e.length = s.duration
      WHERE s.song_id IS NOT NULL'''
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='users',
    insert_stmt='''
        INSERT INTO users ( 
          user_id, 
          first_name, 
          last_name, 
          gender, 
          level 
        ) 
        SELECT 
          DISTINCT(CAST(e.userid AS INT)), 
          e.firstname, 
          e.lastname, 
          e.gender, 
          e.level 
        FROM staging_events AS e 
        WHERE e.userid IS NOT NULL
    '''
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songs',
    insert_stmt='''
        INSERT INTO songs (
          song_id, 
          title, 
          artist_id, 
          year, 
          duration 
        ) 
        SELECT 
          DISTINCT(s.song_id), 
          e.song, 
          s.artist_id, 
          s.year, 
          CAST(e.length AS FLOAT) 
        FROM staging_events AS e 
        JOIN staging_songs AS s ON e.artist = s.artist_name 
          AND e.song = s.title
    '''
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='artists',
    insert_stmt='''
        INSERT INTO artists (
          artist_id, 
          name, 
          location, 
          latitude, 
          longitude 
        ) 
        SELECT 
          DISTINCT(s.artist_id), 
          s.artist_name, 
          s.artist_location, 
          CAST(s.artist_latitude AS FLOAT), 
          CAST(s.artist_longitude AS FLOAT) 
        FROM staging_songs AS s
    '''
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='time',
    insert_stmt='''
        INSERT INTO time (
          start_time, 
          hour, 
          day, 
          week, 
          month, 
          year,
          weekday 
        ) 
        SELECT 
          DISTINCT(s.start_time), 
          EXTRACT (HOUR FROM s.start_time), 
          EXTRACT (DAY FROM s.start_time), 
          EXTRACT (WEEK FROM s.start_time), 
          EXTRACT (MONTH FROM s.start_time), 
          EXTRACT (YEAR FROM s.start_time), 
          EXTRACT (WEEKDAY FROM s.start_time) 
        FROM songplays AS s
    '''
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    query_and_result_checks=[
        {
            'table': 'artists',
            'select_stmt': 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL',
            'result': 0
        },
        {
            'table': 'time',
            'select_stmt': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL',
            'result': 0
        },
        {
            'table': 'songs',
            'select_stmt': 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL',
            'result': 0
        },
        {
            'table': 'users',
            'select_stmt': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL',
            'result': 0
        },
        {
            'table': 'songplays',
            'select_stmt': 'SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL',
            'result': 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [
    stage_events_to_redshift, stage_songs_to_redshift
] >> load_songplays_table >> [
    load_user_dimension_table, 
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table
] >> run_quality_checks >> end_operator