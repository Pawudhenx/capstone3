from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import os
import pandas as pd
import psycopg2
from google.cloud import bigquery

DEFAULT_ARGS = {"owner":"you","retries":1,"retry_delay":timedelta(minutes=5)}
TABLES = ["members","books","loans"]

def extract_hminus1(**context):
    dsn = os.getenv("PG_DSN", "host=postgres user=postgres password=postgres dbname=library port=5432")
    today = date.today()
    start = datetime.combine(today - timedelta(days=1), datetime.min.time())
    end   = datetime.combine(today, datetime.min.time())

    os.makedirs("/opt/airflow/extracts", exist_ok=True)
    out_paths = []
    with psycopg2.connect(dsn) as conn:
        for t in TABLES:
            q = f"SELECT * FROM {t} WHERE created_at >= %s AND created_at < %s"
            df = pd.read_sql(q, conn, params=(start, end))
            path = f"/opt/airflow/extracts/{t}.parquet"
            df.to_parquet(path, index=False)
            out_paths.append(path)

    # opsional: push list file untuk di-inspect
    return out_paths

def load_to_bq(**context):
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    client = bigquery.Client(project=project)

    for t in TABLES:
        path = f"/opt/airflow/extracts/{t}.parquet"
        table_id = f"{project}.{dataset}.{t}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            autodetect=True,
        )
        with open(path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

with DAG(
    dag_id="dag2_daily_postgres_to_bq",
    start_date=datetime(2025,10,1),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["capstone3","bigquery","ingest"],
):
    t_extract = PythonOperator(
        task_id="extract_all_tables_h_minus_1",
        python_callable=extract_hminus1,
        do_xcom_push=True,
    )
    t_load = PythonOperator(
        task_id="load_to_bigquery_incremental_partitioned",
        python_callable=load_to_bq
    )

    t_extract >> t_load
