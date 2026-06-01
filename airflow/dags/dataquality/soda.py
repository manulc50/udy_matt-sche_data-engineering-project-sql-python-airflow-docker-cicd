from airflow.operators.bash import BashOperator
import logging

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

logger = logging.getLogger(__name__)

def create_task_data_quality(schema_name):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema_name}",
            bash_command=f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema_name} {SODA_PATH}/checks.yml"
        )

        return task
    
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema_name}")
        raise e
