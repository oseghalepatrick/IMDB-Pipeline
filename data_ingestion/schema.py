name_schema = types.StructType([
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


movies_details = {
    "name.basics": name_basics,
    "title.akas": title_akas,
    "title.basics": title_basics,
    "title.crew": title_crew,
    "title.episode": title_episode,
    "title.principals": title_principals,
    "title.ratings": title_rating,
}

with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for gcs_name, bq_name in movies_details.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{gcs_name}_data_files_task',
            source_bucket=BUCKET,
            source_object=f'raw/{gcs_name}/*.parquet',
            destination_bucket=BUCKET,
            destination_object=f'{gcs_name}/{gcs_name}_data',
            move_object=True
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{bq_name}_data_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{bq_name}_data",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{gcs_name}/*"],
                },
            },
        )


spark = SparkSession \
    .builder \
    .master('local') \
    .appName('spark-read-from-bigquery') \
    .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.11-0.23.2,com.google.cloud.bigdataoss:gcs-connector-hadoop3-2.2.2-shaded,com.google.guava:guava:r05') \
    .config('spark.jars','guava-11.0.2.jar,gcsio-2.2.5-javadoc.jar') \ # you will have to download these jars manually
    .getOrCreate()