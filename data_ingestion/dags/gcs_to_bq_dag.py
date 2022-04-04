
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

movies_details = {
    "name.basics": "name_basics",
    "title.akas": "title_akas",
    "title.basics": "title_basics",
    "title.crew": "title_crew",
    "title.episode": "title_episode",
    "title.principals": "title_principals",
    "title.ratings": "title_ratings",
}
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'all_movies_data')

INPUT_PATH = 'raw'
INPUT_FILETYPE = "parquet"


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de-project'],
) as dag:

    for gcs_name, bq_name in movies_details.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{gcs_name}_files_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_PATH}/{gcs_name}/*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{gcs_name}/{gcs_name}',
            move_object=True
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{bq_name}_data_table",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{bq_name}_data",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{gcs_name}/*"],
                },
            },
        )

        move_files_gcs_task >> bigquery_external_table_task