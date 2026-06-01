from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

TABLE_NAME = "yt_api_videos"

# Realiza la conexión con Postgres y devuelve la conexión y un cursor para poder interactuar con la base de datos.
def get_connection_cursor():
    # Las conexiones de Airflow externas pueden ser creadas a través de la UI de Airflow(Seccion "Admin" -> "Connections") o creando
    # variables de entorno en el host o máquina de Airflow que tengan el prefijo "AIRFLOW_CONN".
    # En nuestro caso, se han creado la variable de entorno "AIRFLOW_CONN_POSTGRES_DB_YT_ELT" en el docker-compose.yaml.
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")

    connection = hook.get_conn()

    cursor = connection.cursor(cursor_factory=RealDictCursor)

    return connection, cursor


# Cierra el cursor y la conexión con Postgres para liberar recursos. 
def close_connection_cursor(connection, cursor):
    cursor.close()

    connection.close()


def create_schema(schema_name):
    connection, cursor = get_connection_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

    cursor.execute(schema_sql)

    # Realizamos un commit en la base de datos porque estamos realizando un cambio.
    connection.commit()

    close_connection_cursor(connection, cursor)


def create_table(schema_name):
    connection, cursor = get_connection_cursor()

    if schema_name == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{TABLE_NAME} (
                    "id" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "title" TEXT NOT NULL,
                    "upload_date" TIMESTAMP NOT NULL,
                    "duration" VARCHAR(20) NOT NULL,
                    "views" INT,
                    "likes_count" INT,
                    "comments_count" INT
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{TABLE_NAME} (
                    "id" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "title" TEXT NOT NULL,
                    "upload_date" TIMESTAMP NOT NULL,
                    "duration" TIME NOT NULL,
                    "type" VARCHAR(10) NOT NULL,
                    "views" INT,
                    "likes_count" INT,
                    "comments_count" INT
                );
            """
        
    cursor.execute(table_sql)

    # Realizamos un commit en la base de datos porque estamos realizando un cambio.
    connection.commit()

    close_connection_cursor(connection, cursor)


def get_ids(cursor, schema_name):
    cursor.execute(f"SELECT id FROM {schema_name}.{TABLE_NAME};")

    # Ej del retorno: [{"id": "abc123"}, {"id": "xyz456"}]
    video_ids = cursor.fetchall()

    # Ej: ("abc123", "xyz456")
    return [row["id"] for row in video_ids]