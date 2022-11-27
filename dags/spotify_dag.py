from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

AIRFLOW_PROJECT_DIR = '/opt/airflow/dags/'
PYTHON_PROJECT_DIR = '/opt/airflow/repo/streaming_trend'
DBT_PROJECT_DIR = PYTHON_PROJECT_DIR + '/dbt'


DEFAULT_DBT_BRANCH = 'public'
DBT_PROFILE_TARGET = 'dev'
SCHEDULE_INTERVAL = '@hourly'
DAG_CONCURRENCY = 1
DAG_MAX_ACTIVE_RUN = 1

default_args = {
    'owner': 'ha_nguyen',
    'depend_on_past': True,
    'start_date': datetime(2021,6,7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'schedule_interval': '@hourly'
}


DAG_VERSION = 'streaming_trend_dag'
dag = DAG(DAG_VERSION,
          description='dag to get streaming history to postgres database',
          default_args=default_args,
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=DAG_CONCURRENCY,
          max_active_runs=DAG_MAX_ACTIVE_RUN,
          catchup=False
          )

def get_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

with dag:

    collect_spotify_streaming=BashOperator(
        task_id='collect_spotify_streaming',
        bash_command= f'cd {PYTHON_PROJECT_DIR} && python3 main_app.py'
    )

    dbt_seed_to_db = BashOperator(
        task_id='dbt_seed_to_db',
        bash_command=f'cd {DBT_PROJECT_DIR}'
                     f' && dbt seed --profiles-dir {DBT_PROJECT_DIR} --target={DBT_PROFILE_TARGET}'
    )

    dbt_run = BashOperator(
        task_id='dbt_run_all_track_recently_played',
        bash_command=f'cd {DBT_PROJECT_DIR}'
                     f' && dbt run --profiles-dir {DBT_PROJECT_DIR} --target={DBT_PROFILE_TARGET}'
    )

    collect_spotify_streaming >> dbt_seed_to_db >> dbt_run