import os, glob
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

import pyspark
from pyspark.sql import SparkSession, types

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'all_movies_data')


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

def read_data(file_name, dest_file, schema=None):
    url = f'https://datasets.imdbws.com/{file_name}.tsv.gz'
    spark.sparkContext.addFile(url)
    df = spark.read \
            .option('header', 'true') \
            .csv('file://'+pyspark.SparkFiles.get(file_name+'.tsv.gz'), sep='\t', schema=schema)
    df.write.parquet(dest_file, mode='overwrite')

def upload_to_gcs(bucket, file_name):
    print(file_name)
    local_file=glob.glob(f'{file_name}/*.parquet')[0]
    print(local_file)
    base_pth=os.path.basename(local_file)
    object_name=f"raw/{file_name}/{base_pth}"
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def donwload_parquetize_upload_dag(
    dag,
    file_name,
    local_parquet_path_template,
    schema,
    gcs_path_template
):
    with dag:

        format_to_parquet_task = PythonOperator(
            task_id=f"format_{file_name}_to_parquet",
            python_callable=read_data,
            op_kwargs={
                "file_name": file_name,
                "dest_file": local_parquet_path_template,
                "schema":schema
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_{file_name}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "file_name":file_name
            },
        )

        rm_task = BashOperator(
            task_id=f"rm_{file_name}_task",
            bash_command=f"rm -rf {local_parquet_path_template}"
        )

        format_to_parquet_task >> local_to_gcs_task >> rm_task


name_basics_schema = types.StructType([
    types.StructField('nconst',types.StringType(),True),
    types.StructField('primaryName',types.StringType(),True),
    types.StructField('birthYear',types.IntegerType(),True),
    types.StructField('deathYear',types.IntegerType(),True),
    types.StructField('primaryProfession',types.StringType(),True),
    types.StructField('knownForTitles',types.StringType(),True)
])

title_akas_schema = types.StructType([
    types.StructField('titleId',types.StringType(),True),
    types.StructField('ordering',types.IntegerType(),True),
    types.StructField('title',types.StringType(),True),
    types.StructField('region',types.StringType(),True),
    types.StructField('language',types.StringType(),True),
    types.StructField('types',types.StringType(),True),
    types.StructField('attributes',types.StringType(),True),
    types.StructField('isOriginalTitle',types.IntegerType(),True)
])

title_basics_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('titleType',types.StringType(),True),
    types.StructField('primaryTitle',types.StringType(),True),
    types.StructField('originalTitle',types.StringType(),True),
    types.StructField('isAdult',types.IntegerType(),True),
    types.StructField('startYear',types.IntegerType(),True),
    types.StructField('endYear',types.IntegerType(),True),
    types.StructField('runtimeMinutes',types.IntegerType(),True),
    types.StructField('genres',types.StringType(),True)
])

title_crew_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('directors',types.StringType(),True),
    types.StructField('writers',types.StringType(),True)
])

title_episode_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('parentTconst',types.StringType(),True),
    types.StructField('seasonNumber',types.IntegerType(),True),
    types.StructField('episodeNumber',types.IntegerType(),True)
])

title_principals_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('ordering',types.IntegerType(),True),
    types.StructField('nconst',types.StringType(),True),
    types.StructField('category',types.StringType(),True),
    types.StructField('job',types.StringType(),True),
    types.StructField('characters',types.StringType(),True)
])

title_rating_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('averageRating',types.DoubleType(),True),
    types.StructField('numVotes',types.IntegerType(),True)
])

# movies_details = {
#     "name.basics": name_basics_schema,
#     "title.akas": title_akas_schema,
#     "title.basics": title_basics_schema,
#     "title.crew": title_crew_schema,
#     "title.episode": title_episode_schema,
#     "title.principals": title_principals_schema,
#     "title.ratings": title_rating_schema
# }

name_basics = "name.basics"
name_basics_to_gcs_dag = DAG(
    dag_id=f"{name_basics}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=name_basics_to_gcs_dag,
    file_name=name_basics,
    local_parquet_path_template=name_basics,
    schema=name_basics_schema,
    gcs_path_template=name_basics
)

title_akas = "title.akas"
title_akas_to_gcs_dag = DAG(
    dag_id=f"{title_akas}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_akas_to_gcs_dag,
    file_name=title_akas,
    local_parquet_path_template=title_akas,
    schema=title_akas_schema,
    gcs_path_template=title_akas
)

title_basics = "title.basics"
title_basics_gcs_dag = DAG(
    dag_id=f"{title_basics}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_basics_gcs_dag,
    file_name=title_basics,
    local_parquet_path_template=title_basics,
    schema=title_basics_schema,
    gcs_path_template=title_basics
)

title_crew = "title.crew"
title_crew_to_gcs_dag = DAG(
    dag_id=f"{title_crew}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_crew_to_gcs_dag,
    file_name=title_crew,
    local_parquet_path_template=title_crew,
    schema=title_crew_schema,
    gcs_path_template=title_crew
)

title_episode = "title.episode"
title_episode_to_gcs_dag = DAG(
    dag_id=f"{title_episode}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_episode_to_gcs_dag,
    file_name=title_episode,
    local_parquet_path_template=title_episode,
    schema=title_episode_schema,
    gcs_path_template=title_episode
)

title_principals = "title.principals"
title_principals_to_gcs_dag = DAG(
    dag_id=f"{title_principals}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_principals_to_gcs_dag,
    file_name=title_principals,
    local_parquet_path_template=title_principals,
    schema=title_principals_schema,
    gcs_path_template=title_principals
)

title_ratings = "title.ratings"
title_ratings_to_gcs_dag = DAG(
    dag_id=f"{title_ratings}_data_2_gcs_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['de-project'],
)
    
donwload_parquetize_upload_dag(
    dag=title_ratings_to_gcs_dag,
    file_name=title_ratings,
    local_parquet_path_template=title_ratings,
    schema=title_rating_schema,
    gcs_path_template=title_ratings
)