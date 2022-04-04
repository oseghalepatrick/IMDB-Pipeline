For Docker and Airflow Setup, follow the steps [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_2_data_ingestion/airflow/1_setup_official.md)

#### Execution
1. Build the image when there's any change in the `Dockerfile`
```
docker-compose build
```
2. Initialize the Airflow scheduler, DB, and other config
```
docker-compose up airflow-init
```
3. Kick up the all the services from the container:
```
docker-compose up
```
4. Login to Airflow web UI on localhost:8080 with default username: airflow, password: airflow

5. Run your DAG on the Web Console.

6. On finishing your run or to shut down the container/s:
```
docker-compose down
```

[Back to home page](./project/README.md)