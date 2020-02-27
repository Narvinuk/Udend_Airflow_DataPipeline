from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
#import datetime


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 5),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    #'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_project5_dag_backfill',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2018, 11, 1,0,0,0,0),
          end_date=datetime(2018, 11, 5,0,0,0,0),
          schedule_interval='@daily',
          #catchup = False,
          max_active_runs=1          
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    s3_json_key="log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    trucate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=False
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=False
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=False
    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table=["songplays", "users", "songs", "artists", "time"],
    #columns=["start_time","user_id","title","artist_id","start_time"]
    #scripts=[SqlQueries.check_songplay_null,SqlQueries.check_users_null,SqlQueries.check_songs_null,SqlQueries.check_artists_null,SqlQueries.check_artists_null,SqlQueries.check_time_null]
    #scripts=["SqlQueries.check_songplay_null","SqlQueries.check_users_null","SqlQueries.check_songs_null","SqlQueries.check_artists_null","SqlQueries.check_artists_null","SqlQueries.check_time_null"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>stage_events_to_redshift
start_operator >>stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >>  run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator


