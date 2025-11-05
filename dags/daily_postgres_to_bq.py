from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from google.cloud import bigquery
from google.oauth2 import service_account

DEFAULT_ARGS = {"owner":"you","retries":1,"retry_delay":timedelta(minutes=5)}

# Kolom waktu per tabel
TABLES = {
    "members": "created_at",
    "books":   "created_at", 
    "loans":   "created_at",
}

EXTRACT_DIR = "/opt/airflow/extracts"

def extract_hminus1(**_):
    dsn = os.getenv("PG_DSN", "host=postgres user=postgres password=postgres dbname=library port=5432")
    # UTC agar konsisten dengan airflow
    end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=1)

    os.makedirs(EXTRACT_DIR, exist_ok=True)
    out_paths = []

    with psycopg2.connect(dsn) as conn:
        for table, ts_col in TABLES.items():
            q = f"""
                SELECT *
                FROM {table}
                WHERE {ts_col} >= %s AND {ts_col} < %s
            """
            df = pd.read_sql(q, conn, params=(start, end))

            if df.empty:
                print(f"[EXTRACT] {table}: 0 rows (skip write)")
                continue

            path = f"{EXTRACT_DIR}/{table}.parquet"
            df.to_parquet(path, index=False) 
            out_paths.append(path)
            print(f"[EXTRACT] {table}: {len(df)} rows -> {path}")

    return out_paths

def load_to_bq(**_):
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    creds = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    client = bigquery.Client(project=project, credentials=creds)

    # memastikan dataset ada
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    try:
        client.get_dataset(ds_ref)
        print(f"[BQ] Dataset {dataset} exists")
    except Exception:
        client.create_dataset(ds_ref, exists_ok=True)
        print(f"[BQ] Dataset {dataset} created")

    for table, ts_col in TABLES.items():
        path = f"{EXTRACT_DIR}/{table}.parquet"
        if not os.path.exists(path):
            print(f"[LOAD] Skip {table}: file not found (kemungkinan 0 rows kemarin)")
            continue

        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=ts_col,  
            ),
            autodetect=True,
        )

        with open(path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"[LOAD] {table} -> {table_id} (APPEND)")

with DAG(
    dag_id="dag2_daily_postgres_to_bq",
    start_date=datetime(2025, 10, 1),
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
        python_callable=load_to_bq,
    )

    t_extract >> t_load
