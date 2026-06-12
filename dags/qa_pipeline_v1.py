from datetime import datetime

import pymysql
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


STARROCKS_HOST = "host.docker.internal"
STARROCKS_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""
STARROCKS_DATABASE = "qa_lab_v3"


def run_sql_file(sql_file_path: str):
    conn = pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASSWORD,
        database=STARROCKS_DATABASE,
        autocommit=True,
    )

    try:
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_content = file.read()

        statements = [
            stmt.strip()
            for stmt in sql_content.split(";")
            if stmt.strip()
        ]

        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    finally:
        conn.close()


with DAG(
    dag_id="qa_pipeline_v1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["qa", "starrocks", "data-quality"],
):

    raw_to_curado = PythonOperator(
        task_id="raw_to_curado",
        python_callable=run_sql_file,
        op_args=["/opt/airflow/sql/transformations/01_raw_to_curado.sql"],
    )

    curado_to_refinado = PythonOperator(
        task_id="curado_to_refinado",
        python_callable=run_sql_file,
        op_args=["/opt/airflow/sql/transformations/02_curado_to_refinado.sql"],
    )

    raw_to_curado >> curado_to_refinado