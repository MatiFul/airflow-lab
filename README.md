# Airflow Lab para Data QA

Orquestación local del pipeline de [`data-qa-lab`](https://github.com/MatiFul/data-qa-lab)
con Apache Airflow 3. El DAG no duplica las transformaciones: carga RAW, delega el
modelado y sus pruebas a `dbt build`, y ejecuta pytest sólo cuando dbt aprueba.

## DAG principal

`qa_pipeline_postgres_v1` ejecuta cuatro tareas en orden:

```text
create_raw_tables
    → load_raw_postgres
    → run_dbt_build
    → run_pytest_quality_gate
```

Un fallo de dbt bloquea el gate posterior como `upstream_failed`. La última
aceptación local reprodujo las cuatro tareas en `success` con la corrida
`manual__2026-08-10T01:54:06.807302+00:00`.

## Requisitos y estructura

- Docker Desktop con Docker Compose.
- `data-qa-lab` y `airflow-lab` como carpetas hermanas.
- PostgreSQL del laboratorio levantado desde `data-qa-lab`.
- Red Docker compartida `om-lab_app_net`; OpenMetadata no necesita estar activo.

```text
<workspace>/
|-- data-qa-lab/
`-- airflow-lab/
```

Las credenciales incluidas son exclusivamente de demostración local. No deben
reutilizarse en un entorno compartido o productivo.

## Inicio local

Crear una vez la red compartida si todavía no existe:

```powershell
docker network inspect om-lab_app_net *> $null
if ($LASTEXITCODE -ne 0) { docker network create om-lab_app_net }
```

Preparar el archivo local de autenticación de Airflow:

```powershell
Copy-Item .\simple_auth_manager_passwords.example.json .\simple_auth_manager_passwords.json
```

El archivo real está excluido de Git. Después, desde `airflow-lab`:

```powershell
docker compose -f Compose/docker-compose.yaml up --build --detach
```

Interfaz: <http://localhost:8081>. Con el ejemplo local, usuario y contraseña son
`admin`. Antes de disparar el DAG, generar los CSV y levantar PostgreSQL siguiendo
la [guía operativa de data-qa-lab](https://github.com/MatiFul/data-qa-lab/blob/main/docs/GUIA_OPERATIVA.md).

## Validación

Comprobar la sintaxis del DAG y la resolución del Compose:

```powershell
python -m py_compile dags/qa_pipeline_postgres_v1.py
docker compose -f Compose/docker-compose.yaml config --quiet
```

GitHub Actions repite ambas comprobaciones sin levantar el entorno completo. La
ejecución funcional y los quality gates viven en `data-qa-lab`.

## Alcance

Este repositorio contiene la orquestación. La generación de datos, SQL RAW,
proyecto dbt, pytest, API, Playwright, Postman y Power BI pertenecen a
`data-qa-lab`. OpenMetadata es opcional y no forma parte de la publicación de esta
primera versión.
