import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DAGS_DIR = os.path.join(ROOT_DIR, "dags")

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt/profile"
DBT_DEFAULT_PARAMS = (
    f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
)