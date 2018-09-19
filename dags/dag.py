import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from godatadriven.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)

from customops import HttpToGcsOperator
PROJECT_ID = "gdd-eb47dfd7557212651320890d28"
BUCKET = "airflow-training-arjan"


dag = DAG(
    dag_id="my_ninth_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 1, 1),
        "end_date": dt.datetime(2018, 1, 31),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsql_to_gcs",
    postgres_conn_id="pgsql_to_gcs_conn",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="airflow-training-arjan",
    filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json",
    dag=dag,
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
    auto_delete_ttl=5 * 60,  # Autodelete after 5 minutes
)

for currency in {'EUR', 'USD'}:
    https_to_gcs = HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        bucket="airflow-training-arjan",
        dag=dag,
    ) >> dataproc_create_cluster


compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://airflow-training-arjan/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id=PROJECT_ID,
    trigger_rule=TriggerRule.ALL_DONE,
)

flow_to_bq = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        "project": "gdd-airflow-training",
        "region": "europe-west1",
    },
    py_file="gs://airflow-training-arjan/dataflow_job.py",
    dag=dag,
)

gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket=BUCKET,
    source_objects=["average_prices/transfer_date={{ ds }}/*"],
    destination_project_dataset_table="gdd-airflow-training:prices.land_registry_price${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

pgsl_to_gcs >> dataproc_create_cluster
dataproc_create_cluster >> compute_aggregates
compute_aggregates >> dataproc_delete_cluster
dataproc_delete_cluster >> flow_to_bq
dataproc_delete_cluster >> gcs_to_bq
