# pytest -v tests/unit_test.py -k test_api_key
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


# pytest -v tests/unit_test.py -k test_channel_handler
def test_channel_handler(channel_handler):
    assert channel_handler == "MRCHEESE"


# pytest -v tests/unit_test.py -k test_postgres_conn
def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars

    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


# pytest -v -s tests/unit_test.py -k test_dags_integrity
# La opción "-s" es para que se muestren las salidas de los print por consola cuando se ejecute el test.
def test_dags_integrity(dagbag):
    # 1. Comprueba que los DAGS no tienen errores de importación.
    # Si no se cumple la verificación del assert, se muestra el mensaje de error de la derecha.
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=============")
    print(dagbag.import_errors)

    #2. Comprueba que los DAGS tienen unos determinados ids.
    expected_dag_ids = ("produce_json", "update_db", "data_quality")
    load_dag_ids = tuple(dagbag.dags.keys())
    print("=============")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in load_dag_ids, f"DAG {dag_id} is missing"

    # 3. Comprueba que hay un número determinado de DAGs.
    assert dagbag.size() == 3
    print("=============")
    print(dagbag.size())

    # 4. Comprueba que hay un número determinado de tareas en cada uno de los DAGs.
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2
    }

    print("=============")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)

        assert expected_count == actual_count, f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}"

        print(dag_id, len(dag.tasks))