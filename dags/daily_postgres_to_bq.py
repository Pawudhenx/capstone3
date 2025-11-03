from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import os
import pandas as pd
import psycopg2
from google.cloud import bigquery

DEFAULT_ARGS = {"owner":"you","retries":1,"retry_delay":timedelta(minutes=5)}
TABLES = ["members","books","loans"]  # extract semua tabel dalam 1 fungsi

# ENV yang perlu kamu set di docker-compose Airflow:
# GOOGLE_APPLICATION_CREDENTIALS=/keys/sa.json
# BQ_PROJECT=purwadika   (sesuai guideline)
# BQ_DATASET=namaAnda_perpustakaan_capstone3

def extract_hminus1(**context):
    dsn = os.getenv("PG_DSN", "host=postgres user=postgres password=postgres dbname=library port=5432")
    # H-1 window (UTC, cocokkan jika ingin WIB)
    today = date.today()
    start = datetime.combine(today - timedelta(days=1), datetime.min.time())
    end   = datetime.combine(today, datetime.min.time())

    os.makedirs("/tmp/extracts", exist_ok=True)
    with psycopg2.connect(dsn) as conn:
        for t in TABLES:
            q = f"""
              SELECT * FROM {t}
              WHERE created_at >= %s AND created_at < %s
            """
            df = pd.read_sql(q, conn, params=(start, end))
            df.to_parquet(f"/tmp/extracts/{t}.parquet", index=False)

    # simpan path untuk task berikutnya
    return [f"/tmp/extracts/{t}.parquet" for t in TABLES]

def load_to_bq(**context):
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    client = bigquery.Client(project=project)

    # Partitioning by created_at (time partition), write append = incremental
    for t in TABLES:
        path = f"/tmp/extracts/{t}.parquet"
        table_id = f"{project}.{dataset}.{t}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            autodetect=True,  # biar ramah pemula
        )
        with open(path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()  # tunggu selesai

with DAG(
    dag_id="daily_postgres_to_bq",
    start_date=datetime(2025,10,1),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["capstone3","bq","ingest"],
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
