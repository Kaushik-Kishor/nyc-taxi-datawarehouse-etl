from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="nyc_taxi_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "dbt", "warehouse"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /opt/airflow/scripts/ingest_taxi_to_postgres.py"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/taxi_dbt && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/taxi_dbt && dbt test"
    )

    ingest >> dbt_run >> dbt_test
