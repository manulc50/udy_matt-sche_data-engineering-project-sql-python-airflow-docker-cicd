from datawarehouse.data_utils import get_connection_cursor, close_connection_cursor, create_schema, create_table, get_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modifications import insert_row, update_row, delete_rows
from datawarehouse.data_transformation import transform_data

from airflow.decorators import task

import logging


TABLE_NAME = "yt_api_videos"

logger = logging.getLogger(__name__)

@task
def update_staging_table():
    schema_name = 'staging'

    try:
        create_schema(schema_name)

        create_table(schema_name)

        json_videos_data = load_data()

        connection, cursor = get_connection_cursor()

        video_ids = get_ids(cursor, schema_name)

        # Insertar o actualizar.
        for json_video_data in json_videos_data:
            if len(video_ids) == 0:
                insert_row(connection, cursor, schema_name, json_video_data)
            else:
                if json_video_data['video_id'] in video_ids:
                    update_row(connection, cursor, schema_name, json_video_data)
                else:
                    insert_row(connection, cursor, schema_name, json_video_data)

        # Eliminar.

        # Usamos un conjunto para no tener ids duplicados.
        ids_in_json = {json_video_data['video_id'] for json_video_data in json_videos_data}

        ids_to_delete = set(video_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(connection, cursor, schema_name, ids_to_delete)

        logger.info(f"Schema {schema_name} update completed")
    
    except Exception as e:
        logger.error(f"An error occurred during the update of {schema_name} table: {e}")
        raise e

    finally:
        if connection and cursor:
            close_connection_cursor(connection, cursor)


@task
def update_core_table():
    schema_name = 'core'

    try:
        create_schema(schema_name)

        create_table(schema_name)

        connection, cursor = get_connection_cursor()

        video_ids = get_ids(cursor, schema_name)

        cursor.execute("SELECT * FROM staging.{};".format(TABLE_NAME))
        rows = cursor.fetchall()

        staging_video_ids = set()

        # Insertar o actualizar.
        for row in rows:
            staging_video_ids.add(row['id'])

            transformed_row = transform_data(row)

            if len(video_ids) == 0:
                insert_row(connection, cursor, schema_name, transformed_row)
            else:
                if row['id'] in video_ids:
                    update_row(connection, cursor, schema_name, transformed_row)
                else:
                    insert_row(connection, cursor, schema_name, transformed_row)

        # Eliminar.
        ids_to_delete = set(video_ids) - staging_video_ids

        if ids_to_delete:
            delete_rows(connection, cursor, schema_name, ids_to_delete)

        logger.info(f"Schema {schema_name} update completed")
    
    except Exception as e:
        logger.error(f"An error occurred during the update of {schema_name} table: {e}")
        raise e

    finally:
        if connection and cursor:
            close_connection_cursor(connection, cursor)