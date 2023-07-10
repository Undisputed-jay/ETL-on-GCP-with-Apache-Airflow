from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator


default_arguments = {
    'owner': 'Ahmed ayodele',
    'start_date': datetime(2023, 6, 9),
    'retries': 1,
    'retry_delay' : timedelta(minutes = 5)
}

# GCSHook is used by Google cloud to interact with google cloud storage
def list_objects(source_bucket = None):
    hook = GCSHook()
    storage_objects = hook.list(source_bucket)
    
    return storage_objects
    
# moving files from source bucket to the destination bucket and deleting it from source bucket
def move_object(source_bucket=None, destination_bucket=None, prefix=None, **kwargs):
    storage_objects = kwargs['ti'].xcom_pull(task_ids='list_bucket_object')
    hook = GCSHook()
    
    print("Retrieved storage objects:", storage_objects)
    
    for storage_object in storage_objects:
        destination_object = storage_object
        
        if prefix:                  #if a prefix is provided, append it to the back of object
            destination_object = "{}/{}".format(prefix, storage_object)
        hook.copy(source_bucket, storage_object, destination_bucket)
        hook.delete(source_bucket, storage_object)


with DAG(
    'bigquery_data_load',
    schedule_interval = '@hourly',
    catchup = False,
    default_args = default_arguments,
    max_active_runs = 1,
    user_defined_macros = {'project': 'aoa-airflow-tutorial'} 
) as dag:
    
    
    view_object = PythonOperator(
        task_id = 'list_bucket_object',
        python_callable = list_objects,
        op_kwargs = {'source_bucket': 'aoa-logistics-landing-bucket'},
        provide_context = True
    )
    
    
    load_data = GCSToBigQueryOperator(
        task_id='load_data',
        bucket='aoa-logistics-landing-bucket',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table = 'aoa-airflow-tutorial.vehicleanalytics.history',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default',
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
        FROM `aoa-airflow-tutorial.vehicleanalytics.history`) as latest
    WHERE rank = 1;
    """
    
    create_table = BigQueryExecuteQueryOperator(
        task_id = 'create_table',
        sql = query,
        destination_dataset_table = 'aoa-airflow-tutorial.vehicleanalytics.latest',
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        use_legacy_sql = False,
        location = 'europe-west2',
        gcp_conn_id='google_cloud_default'
    )
    
    move_files = PythonOperator(
        task_id = 'move_files',
        python_callable = move_object,
        op_kwargs = {'source_bucket': 'aoa-logistics-landing-bucket',
                     'destination_bucket': 'aoa-logistics-backup-bucket',
                     'prefix': '{{ ts_nodash }}'},
        provide_context = True,
    )
    
view_object >> load_data >> create_table >> move_files

    
