import requests
import psycopg2
import pytest


# pytest -v tests/integration_test.py -k test_youtube_api_response
def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("api_key")
    channel_handler = airflow_variable("channel_handler")
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handler}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200

    except requests.RequestException as e:
        pytest.fail(f"Request to Youtube API failed: {e}")


# pytest -v tests/integration_test.py -k test_real_postgres_connection
def test_real_postgres_connection(real_postgres_connection):
    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        results = cursor.fetchone() # results -> (1,)

        assert results[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor:
            cursor.close()