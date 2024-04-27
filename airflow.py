from airflow import DAG
from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator

#Default arguments for the DAG
default_args = {
    'owner': 'Sakshigatyan1998',
    'start_date': datetime(2023, 12, 3),
    'retries': 0,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'project-id',  #hiding project-id
        'runner': 'DataflowRunner'
    }
}

def list_files(bucket_name, prefix, processed_prefix='processed/'):
    gcs_hook = GoogleCloudStorageHook()
    files = gcs_hook.list(bucket_name, prefix=prefix)
    if files:
        #Move the file to the 'processed' subdirectory
        source_object = files[0]
        file_name = source_object.split('/')[-1]  #Get the file name
        destination_object = processed_prefix + file_name
        gcs_hook.copy(bucket_name, source_object, bucket_name, destination_object)
        gcs_hook.delete(bucket_name, source_object)
        return destination_object
    else:
        return None

#Define the DAG
with models.DAG('ecc_project_dag',
                default_args=default_args,
                schedule_interval='*/10 * * * *',  #Run every 10 minutes
                catchup=False,
                max_active_runs=1) as dag:  #Limit to one active run at a time

    gcs_sensor = GoogleCloudStoragePrefixSensor(
        task_id='gcs_sensor',
        bucket='ecc-food-delivery', #Add bucket name
        prefix='food_daily',
        mode='poke',
        poke_interval=60,  #Check every 60 seconds
        timeout=300  #Stop after 5 minutes if no file is found
    )

    list_files_task = PythonOperator(
        task_id='list-files-in-GCS',
        python_callable=list_files,
        op_kwargs={'bucket_name': 'ecc-food-delivery', 'prefix': 'food_daily'}, #Add bucket name
        do_xcom_push=True,  #This will push the return value of list_files to XCom
    )

    beamtask = DataFlowPythonOperator(
        task_id='beamtask',
        #Path to the Beam pipeline file
        py_file= 'gs://us-central1-ecc-composer-ed51380b-bucket/beam.py'     #'gs://composer-bucket/beam.py',
        #Input file for the pipeline
        options={'input': 'gs://ecc-food-delivery/{{ task_instance.xcom_pull("list_files") }}'}
    )

    gcs_sensor >> list-files-in-GCS >> beamtask 