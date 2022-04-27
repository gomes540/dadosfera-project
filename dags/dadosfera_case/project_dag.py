# [START documentation]
# set up connectivity from airflow to gcp using [key] in json format
# create new bucket - dadosfera-data-lake [GCSCreateBucketOperator] 
# create new bucket - dadosfera-landing-zone [GCSCreateBucketOperator]
# create new bucket - dadosfera-processing-zone [GCSCreateBucketOperator]
# create new bucket - dadosfera-curated-zone [GCSCreateBucketOperator]
# sync files from dadosfera-landing-zone to dadosfera-processing-zone [GCSSynchronizeBucketsOperator]
# list objects on the processing zone [GCSListObjectsOperator]
# create google cloud dataproc cluster - spark engine [DataprocCreateClusterOperator]
# submit pyspark job top google cloud dataproc cluster [DataprocSubmitPySparkJobOperator]
# configure sensor to guarantee completeness of pyspark job [DataprocJobSensor]
# create dataset on bigquery [BigQueryCreateEmptyDatasetOperator]
# verify count of rows (if no null) [BigQueryCheckOperator]
# deletet google cloud dataproc cluster [DataprocDeleteClusterOperator]
# delete bucket dadosfera-processing-zone [GCSDeleteBucketOperator]
# [END documentation]


# [START import module]
from asyncio import tasks
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_operator import GoogleCloudStorageCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSBucketCreateAclEntryOperator, GCSSynchronizeBucketsOperator, GCSListObjectsOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from dadosfera_case.gcs_loader import upload_trips_files_to_gcs
# [END import module]


# [START import variables]
LANDING_BUCKET_ZONE = Variable.get("dadosfera_landing_zone_bucket")
PROCESSING_BUCKET_ZONE = Variable.get("dadosfera_processing_zone_bucket")
CURATED_BUCKET_ZONE = Variable.get("dadosfera_curated_zone_bucket")
BUCKET_LOCATION = Variable.get("dadosfera_bucket_location")
# [END import variables]


# [START default args]
default_args = {
    'owner': 'Felipe Gomes',
    'depends_on_past': False
}
# [END default args]

# [START instantiate dag]
with DAG(
    dag_id="gcp-gcs-dataproc-bigquery-dadosfera-case",
    tags=['development', 'cloud storage', 'cloud dataproc', 'google bigqueury', 'pyspark', 'dadosfera'],
    default_args=default_args,
    start_date=datetime(year=2022, month=4, day=26),
    schedule_interval='@daily',
    catchup=False,
    description="ETL Process for Dadosfera Case"
) as dag:
# [END instantiate dag]

# [START set tasks]
    # create start task
    start = DummyOperator(task_id="start")

    # create end task
    end = DummyOperator(task_id="end")
    
    # create gcp bucket to dadosfera landing zone - dadosfera-landing-zone
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/gcs.html
    
    create_gcs_dadosfera_landing_zone = GoogleCloudStorageCreateBucketOperator(
        task_id="create_gcs_dadosfera_landing_zone_bucket",
        bucket_name=LANDING_BUCKET_ZONE,
        storage_class='STANDARD',
        location=BUCKET_LOCATION,
        labels={'env': 'dev', 'team': 'airflow'},
        gcp_conn_id="gcp_dadosfera"
    )
    
    # create gcp bucket to dadosfera processing zone - dadosfera-processing-zone
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/gcs.html
    
    create_gcs_dadosfera_processing_zone = GoogleCloudStorageCreateBucketOperator(
        task_id="create_gcs_dadosfera_processing_zone_bucket",
        bucket_name=PROCESSING_BUCKET_ZONE,
        storage_class='STANDARD',
        location=BUCKET_LOCATION,
        labels={'env': 'dev', 'team': 'airflow'},
        gcp_conn_id="gcp_dadosfera"
    )
    
    # create gcp bucket to dadosfera curated zone - dadosfera-curated-zone
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/gcs.html
    
    create_gcs_dadosfera_curated_zone = GoogleCloudStorageCreateBucketOperator(
        task_id="create_gcs_dadosfera_curated_zone_bucket",
        bucket_name=CURATED_BUCKET_ZONE,
        storage_class='STANDARD',
        location=BUCKET_LOCATION,
        labels={'env': 'dev', 'team': 'airflow'},
        gcp_conn_id="gcp_dadosfera"
    )
    
    # transfer local data to landing bucket zone - dadosfera-landing-zone
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#airflow.operators.python.PythonOperator
    upload_local_file = PythonOperator(
        task_id='transfer_data_trips_to_landing_bucket_zone',
        python_callable=upload_trips_files_to_gcs,
        provide_context=True,
        op_kwargs={
            "project_id":Variable.get("dadosfera_project_id"),
            "bucket":LANDING_BUCKET_ZONE,
            "dadosfera_service_account":Variable.get("dadosfera_sa_secret")
        }
    )
    # sync files from one bucket to another bucket - dadosfera-landing-zone to dadosfera-processing-zone
    gcs_sync_trips_landing_to_processing_zone = GCSSynchronizeBucketsOperator(
        task_id="gcs_sync_trips_landing_to_processing_zone",
        source_bucket=LANDING_BUCKET_ZONE,
        source_object="trips/",
        destination_bucket=PROCESSING_BUCKET_ZONE,
        destination_object="trips/",
        allow_overwrite=True,
        gcp_conn_id="gcp_dadosfera"
    )
    
    # list files inside of gcs bucket - processing zone
    # https://registry.astronomer.io/providers/google/modules/gcslistobjectsoperator
    list_files_processing_zone = GCSListObjectsOperator(
        task_id="list_files_processing_zone",
        bucket=PROCESSING_BUCKET_ZONE,
        gcp_conn_id="gcp_dadosfera"
    )
    
    # list files inside of gcs bucket - processing zone
    # https://registry.astronomer.io/providers/google/modules/gcslistobjectsoperator
    list_files_landing_zone = GCSListObjectsOperator(
        task_id="list_files_landing_zone",
        bucket=LANDING_BUCKET_ZONE,
        gcp_conn_id="gcp_dadosfera"
    )

# [END set tasks]

# [START task sequence]
start >> [create_gcs_dadosfera_landing_zone, create_gcs_dadosfera_processing_zone, create_gcs_dadosfera_curated_zone] >> upload_local_file >> gcs_sync_trips_landing_to_processing_zone >> [list_files_landing_zone, list_files_processing_zone] >> end
# [END task sequence]
