import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stocks_data')

DATASET = "stcdata"
TICKERS = {'AMAZON': 'data_AMZN', 'FB': 'data_FB', 'TESLA': 'data_TSLA', 'APPLE': 'data_AAPL'}
INPUT_PART = "tier2"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for ticker, ds_col in TICKERS.items():

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{ticker}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{ticker}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{INPUT_PART}/{ticker}/*"],
                },
            },
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{ticker}_{DATASET} \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{ticker}_{DATASET}_external_table;"
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{ticker}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        bigquery_external_table_task >> bq_create_partitioned_table_job