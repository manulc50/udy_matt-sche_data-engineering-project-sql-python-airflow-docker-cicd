from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, get_videos_data, save_to_json
from datawarehouse.dwh import update_staging_table, update_core_table
from dataquality.soda import create_task_data_quality

# Define the local timezone.
local_tz = pendulum.timezone("Europe/Madrid")

# Default DAG args.
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    #"retries": 1,
    #"retry_delay: timedelta(minutes=5)",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1)
}

with DAG(
    dag_id="produce_json",
    description="DAG to produce JSON file with raw data",
    default_args=default_args,
    schedule="0 14 * * *",
    catchup=False,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    #end_date=datetime(2030, 12, 12, tzinfo=local_tz)
) as dag:
    
    # Define tasks.
    # Nota: En este caso, no hace falta usar operadores de Airflow aquí porque cada una de las funciones
    # de Python que usamos aquí están anotadas con el decorador @task de Airflow (ver script "video_stats.py").
    playlist_id = get_playlist_id()
    videos_ids = get_video_ids(playlist_id)
    videos_data = get_videos_data(videos_ids)
    videos_data_json = save_to_json(videos_data)

    # Tarea que utiliza el operador TriggerDagRunOperator para disparar o lanzar el DAG con id "update_db".
    trigger_dag_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db"
    )

    # Define dependencies.
    # Nota: Como la tarea "trigger_dag_update_db" depende de todas las anteriores, el DAG con id "update_db" solo se
    # disparará o lanzará si todas esas tareas anteriores finalizan correctamente.
    playlist_id >> videos_ids >> videos_data >> videos_data_json >> trigger_dag_update_db

with DAG(
    dag_id="update_db",
    description="DAG to process JSON file and insert data into staging and core schemas",
    default_args=default_args,
    schedule=None, # Ya que este DAG es lanzado por el DAG anterior con id "produce_json".
    catchup=False
) as dag:
    
    # Define tasks.
    # Nota: En este caso, no hace falta usar operadores de Airflow aquí porque cada una de las funciones
    # de Python que usamos aquí están anotadas con el decorador @task de Airflow (ver script "dwh.py").
    update_staging = update_staging_table()
    update_core = update_core_table()

    # Tarea que utiliza el operador TriggerDagRunOperator para disparar o lanzar el DAG con id "data_quality".
    trigger_dag_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality"
    )

    # Define dependencies.
    # Nota: Como la tarea "trigger_dag_data_quality" depende de todas las anteriores, el DAG con id "data_quality" solo se
    # disparará o lanzará si todas esas tareas anteriores finalizan correctamente.
    update_staging >> update_core >> trigger_dag_data_quality

with DAG(
    dag_id="data_quality",
    description="DAG to check the data quality on both layers or schemas in the database",
    default_args=default_args,
    schedule=None, # Ya que este DAG es lanzado por el DAG anterior con id "update_db".
    catchup=False
) as dag:
    
    # Define tasks.
    staging_soda_validation = create_task_data_quality("staging")
    core_soda_validation = create_task_data_quality("core")

    # Define dependencies.
    staging_soda_validation >> core_soda_validation