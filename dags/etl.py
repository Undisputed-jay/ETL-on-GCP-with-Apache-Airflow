from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


default_argument = {
    'owner': 'airflow',
    'start_date' : datetime(2023, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes = 5)
}

with DAG(
    dag_id = 'bigquery_data_load_transform',
    schedule = '@daily',
    default_args = default_argument,
    catchup = False
) as dag:
    
    # Moving files from local drive to Google cloud Storage
    t1 = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_google_cloud_storage',
        src = '/opt/airflow/data/*.csv',
        dst = 'data/',
        bucket = 'landing-bucket-airflow',
        gcp_conn_id = 'google_cloud'
    )
    
    # Moving files to BigQuery from Google cloud Storage
    t2 = GCSToBigQueryOperator(
        task_id = 'load_to_BigQuery',
        bucket = 'landing-bucket-airflow',
        source_objects=['data/*'],
        destination_project_dataset_table = 'aoa-airflow-tutorial.analytics.history',
        gcp_conn_id = 'google_cloud',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        create_disposition='CREATE_IF_NEEDED', # If the table already exists, this parameter has no effect. It ensures that the table is created if it's missing before the write operation is performed.
        write_disposition='WRITE_APPEND', #WRITE_APPEND' means that the data will be appended to the existing data in the table if it already exists. If the table does not exist, it will be created (as determined by the create_disposition
        location = 'europe-west2',
    )
    
    query = """
        SELECT * except(rank)
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY vehicle_id ORDER BY DATETIME(date, TIME(hour, minute, 0)) DESC
            ) as rank 
            FROM `aoa-airflow-tutorial.analytics.history`) as latest
        WHERE rank = 1;
    """
    # This runs the query and saves to a new table (latest)
    t3 = BigQueryExecuteQueryOperator(
        task_id = 'run_query_n_create_table',
        sql = query,
        destination_dataset_table = 'aoa-airflow-tutorial.analytics.latest',
        write_disposition = 'WRITE_TRUNCATE', # the existing data in the table will be deleted and replaced with the new data being written.
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False, # dont use bigquery sql format rather use the normal sql to run the query
        location = 'europe-west2',
        gcp_conn_id='google_cloud'
    )
    
    t4 = BigQueryToGCSOperator(
        task_id ='move_from_bigquery_to_gcs',
        source_project_dataset_table = "aoa-airflow-tutorial:analytics.latest",
        destination_cloud_storage_uris=['gs://landing-bucket-airflow/processed_files/data.csv'],
        project_id = 'aoa-airflow-tutorial',
        gcp_conn_id = 'google_cloud',
        export_format='CSV'
    )
    
    t1 >> t2 >> t3 >> t4