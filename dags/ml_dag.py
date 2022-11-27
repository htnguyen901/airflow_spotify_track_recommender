from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import papermill as pm
import os

AIRFLOW_PROJECT_DIR = '/opt/airflow/dags/'
PYTHON_PROJECT_DIR = '/opt/airflow/repo/streaming_trend'
DBT_PROJECT_DIR = PYTHON_PROJECT_DIR + '/dbt'
JUPYTER_NOTEBOOK = PYTHON_PROJECT_DIR + '/recommender_playlists.ipynb'
PYTHON_COLLECT_TRACK_DIR = '/opt/airflow/repo/streaming_trend/data_collection'


DEFAULT_DBT_BRANCH = 'public'
DBT_PROFILE_TARGET = 'dev'
SCHEDULE_INTERVAL = '@daily'
DAG_CONCURRENCY = 1
DAG_MAX_ACTIVE_RUN = 1

execution_date=datetime.now().strftime("%d_%m_%Y%H%M%S")

def ml_run_notebook():
    print(os.getcwd)
    pm.execute_notebook(
        f'{JUPYTER_NOTEBOOK}',
        f'{PYTHON_PROJECT_DIR}/out_notebook/out-{ execution_date }.ipynb',
        parameters={"execution_date": execution_date, "python_project_dir": PYTHON_PROJECT_DIR }
    )

default_args = {
    'owner': 'ha_nguyen',
    'depend_on_past': True,
    'start_date': datetime(2021,6,7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'schedule_interval': '@daily'
}

DAG_VERSION = 'ml_spotify_recommendation_track_dag'
dag = DAG(DAG_VERSION,
          description='dag to run ML notebook & add/update playlist in spotify',
          default_args=default_args,
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=DAG_CONCURRENCY,
          max_active_runs=DAG_MAX_ACTIVE_RUN,
          catchup=False
          )


with dag:

    collect_all_tracks=BashOperator(
        task_id='collect_all_tracks_saved_playlist',
        bash_command= f'cd {PYTHON_COLLECT_TRACK_DIR} && python3 all_song_pull.py'
    )
