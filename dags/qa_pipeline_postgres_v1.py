from __future__ import annotations

from datetime import datetime
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


POSTGRES_CONN = {
    "host": "qa_lab_postgres",
    "port": 5432,
    "dbname": "qa_lab",
    "user": "qa_user",
    "password": "qa_pass",
}

SQL_BASE_PATH = Path("/opt/airflow/sql/postgres")
DATA_PATH = Path("/opt/airflow/data")


def run_sql_file(relative_path: str) -> None:
    sql_path = SQL_BASE_PATH / relative_path

    with open(sql_path, "r", encoding="utf-8") as file:
        sql = file.read()

    with psycopg2.connect(**POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def load_csv_to_raw() -> None:
    files = [
        ("clientes_raw.csv", "raw.clientes_raw"),
        ("cuentas_raw.csv", "raw.cuentas_raw"),
        ("productos_raw.csv", "raw.productos_raw"),
        ("estados_transaccion_raw.csv", "raw.estados_transaccion_raw"),
        ("canales_raw.csv", "raw.canales_raw"),
        ("sucursales_raw.csv", "raw.sucursales_raw"),
        ("transacciones_raw.csv", "raw.transacciones_raw"),
        ("items_transaccion_raw.csv", "raw.items_transaccion_raw"),
    ]

    with psycopg2.connect(**POSTGRES_CONN) as conn:
        with conn.cursor() as cur:
            for _, table_name in files:
                cur.execute(f"TRUNCATE TABLE {table_name};")

            for file_name, table_name in files:
                file_path = DATA_PATH / file_name

                with open(file_path, "r", encoding="utf-8") as csv_file:
                    cur.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true)",
                        csv_file,
                    )


with DAG(
    dag_id="qa_pipeline_postgres_v1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-qa-lab", "postgres", "lineage"],
) as dag:

    create_raw_tables = PythonOperator(
        task_id="create_raw_tables",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "ddl/raw/01_create_raw_tables.sql"},
    )

    create_curado_tables = PythonOperator(
        task_id="create_curado_tables",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "ddl/curado/01_create_curado_tables.sql"},
    )

    create_refinado_tables = PythonOperator(
        task_id="create_refinado_tables",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "ddl/refinado/01_create_refinado_tables.sql"},
    )

    create_consumo_tables = PythonOperator(
        task_id="create_consumo_tables",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "ddl/consumo/01_create_consumo_tables.sql"},
    )

    load_raw_postgres = PythonOperator(
        task_id="load_raw_postgres",
        python_callable=load_csv_to_raw,
    )

    raw_to_curado_postgres = PythonOperator(
        task_id="raw_to_curado_postgres",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "transformations/01_raw_to_curado.sql"},
    )

    curado_to_refinado_postgres = PythonOperator(
        task_id="curado_to_refinado_postgres",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "transformations/02_curado_to_refinado.sql"},
    )

    refinado_to_consumo_postgres = PythonOperator(
        task_id="refinado_to_consumo_postgres",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "transformations/03_refinado_to_consumo.sql"},
    )

    apply_postgres_documentation = PythonOperator(
        task_id="apply_postgres_documentation",
        python_callable=run_sql_file,
        op_kwargs={"relative_path": "documentation/01_column_comments.sql"},
    )

    (
        create_raw_tables
        >> create_curado_tables
        >> create_refinado_tables
        >> create_consumo_tables
        >> load_raw_postgres
        >> raw_to_curado_postgres
        >> curado_to_refinado_postgres
        >> refinado_to_consumo_postgres
        >> apply_postgres_documentation
    )