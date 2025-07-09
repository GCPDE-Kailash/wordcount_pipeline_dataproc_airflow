from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Replace these values
BUCKET = "airflow-wordcount-bucket"
REGION = "us-central1"
CLUSTER_NAME = "cluster-demo"
PROJECT_ID = "gcp-lead-developer"
BQ_DATASET = "wordcount_ds"
BQ_TABLE = "wordcount_result_airflow_dataproc"

# PySpark Job Config
PYSPARK_URI = f"gs://{BUCKET}/scripts/wordcount.py"
OUTPUT_URI = f"gs://{BUCKET}/output/fixed_wordcount_result"

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with models.DAG(
    dag_id='wordcount_dataproc_to_bigquery',
    schedule_interval=None,  # on demand
    default_args=default_args,
    catchup=False,
    description='Run wordcount.py on Dataproc and load result to BQ',
) as dag:

    # Task 1: Submit PySpark Job
    wordcount_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
        },
    }

    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_wordcount_job",
        job=wordcount_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task 2: Load GCS → BigQuery
    load_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load_wordcount_to_bq",
        bucket=BUCKET,
        source_objects=["output/fixed_wordcount_result/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        schema_fields=[
            {"name": "word", "type": "STRING", "mode": "NULLABLE"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        skip_leading_rows=0,
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
    )

    run_dataproc_job >> load_to_bq
