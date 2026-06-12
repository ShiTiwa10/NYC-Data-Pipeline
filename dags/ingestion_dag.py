from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "nyc_context_engine_monthly_ingestion",
    default_args=default_args,
    description="Executes the monthly data ELT for the Medallion architecture.",
    schedule_interval="@monthly",
    catchup=True,
    tags=["ingestion", "bronze", "silver", "gold"],
) as dag:

    # 1. Extract & Load (Python)
    run_ingestors = BashOperator(
        task_id="run_main_ingestion_script",
        bash_command=(
            "PYTHONPATH=/opt/airflow/src python /opt/airflow/src/main_ingestion.py "
            "--year {{ logical_date.year }} "
            "--month {{ logical_date.month }}"
        ),
    )

    # 2. Transform (dbt)
    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir .",
    )

    run_ingestors >> run_dbt
