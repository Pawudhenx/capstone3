from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, sys
from pathlib import Path

# supaya bisa import modul di folder app/
sys.path.append(str(Path(__file__).resolve().parents[1] / "app"))

from load_from_csv import main as load_csv_main  # simple & ramah pemula

DEFAULT_ARGS = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="hourly_insert_postgres_library",
    start_date=datetime(2025, 10, 1),
    schedule="0 * * * *",     # setiap jam
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["capstone3","postgres","dummy"],
):
    def _run():
        # jika Airflow & Postgres satu jaringan docker, host=postgres
        os.environ["PG_DSN"] = os.getenv("PG_DSN", "host=postgres user=postgres password=postgres dbname=library port=5432")
        load_csv_main()

    insert_task = PythonOperator(
        task_id="generate_and_insert_dummy",
        python_callable=_run
    )
