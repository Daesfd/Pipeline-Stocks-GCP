import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

import yfinance as yf
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def download_data(symbol, output_file):
    prices = yf.download(symbol, period="max")
    return prices.to_csv(output_file)


def basic_transformation(input_file, output_file):
    df = pd.read_csv(input_file)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Open'] = df['Open'].astype('float32')
    df['High'] = df['High'].astype('float32')
    df['Low'] = df['Low'].astype('float32')
    df['Close'] = df['Close'].astype('float32')
    df['Adj Close'] = df['Adj Close'].astype('float32')
    df['Volume'] = df['Volume'].astype('int32')

    table = pv.read_csv(input_file)
    pq.write_table(table, output_file)


def upload_to_gcs(bucket, object_name, input_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(input_file)


default_args = {
    "owner": "root",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def donwload_parquetize_upload_dag(
        dag,
        stock_name,
        local_csv_path_template,
        local_parquet_path_template_modfied,
        tier1_gcs_path_template,
        tier2_gcs_path_template
):
    with dag:
        download_dataset_task = PythonOperator(
            task_id="download_dataset_task",
            python_callable=download_data,
            op_kwargs={
                "symbol": stock_name,
                "output_file": local_csv_path_template,
            }
        )

        local_to_gcs_tier1_task = PythonOperator(
            task_id="local_to_gcs_tier1_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": tier1_gcs_path_template,
                "input_file": local_csv_path_template,
            },
        )

        basic_transformation_task = PythonOperator(
            task_id="basic_transformation_task",
            python_callable=basic_transformation,
            op_kwargs={
                "symbol": stock_name,
                "input_file": local_csv_path_template,
                "output_file": local_parquet_path_template_modfied,
            }
        )

        local_to_gcs_tier2_task = PythonOperator(
            task_id="local_to_gcs_tier2_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": tier2_gcs_path_template,
                "input_file": local_parquet_path_template_modfied,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template_modfied}"
        )

        download_dataset_task >> local_to_gcs_tier1_task >> basic_transformation_task >> local_to_gcs_tier2_task >> rm_task


TSLA_CSV_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_TSLA.csv'
TSLA_PARQUET_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_TSLA_modified.parquet'
TSLA_GCS_PATH_TEMPLATE_TIER1 = "tier1/TESLA/data_TSLA.csv"
TSLA_GCS_PATH_TEMPLATE_TIER2 = "tier2/TESLA/data_TSLA.parquet"

Tesla_data_dag = DAG(
    dag_id="Tesla_data",
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=Tesla_data_dag,
    stock_name="TSLA",
    local_csv_path_template=TSLA_CSV_LOCAL_FILE_TEMPLATE,
    local_parquet_path_template_modfied=TSLA_PARQUET_LOCAL_FILE_TEMPLATE,
    tier1_gcs_path_template=TSLA_GCS_PATH_TEMPLATE_TIER1,
    tier2_gcs_path_template=TSLA_GCS_PATH_TEMPLATE_TIER2
)


FB_CSV_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_FB.csv'
FB_PARQUET_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_FB_modified.parquet'
FB_GCS_PATH_TEMPLATE_TIER1 = "tier1/FB/data_FB.csv"
FB_GCS_PATH_TEMPLATE_TIER2 = "tier2/FB/data_FB.parquet"

FB_data_dag = DAG(
    dag_id="FB_data",
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=FB_data_dag,
    stock_name="FB",
    local_csv_path_template=FB_CSV_LOCAL_FILE_TEMPLATE,
    local_parquet_path_template_modfied=FB_PARQUET_LOCAL_FILE_TEMPLATE,
    tier1_gcs_path_template=FB_GCS_PATH_TEMPLATE_TIER1,
    tier2_gcs_path_template=FB_GCS_PATH_TEMPLATE_TIER2
)

AMZN_CSV_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_AMZN.csv'
AMZN_PARQUET_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_AMZN_modified.parquet'
AMZN_GCS_PATH_TEMPLATE_TIER1 = "tier1/AMAZON/data_AMZN.csv"
AMZN_GCS_PATH_TEMPLATE_TIER2 = "tier2/AMAZON/data_AMZN.parquet"

AMZN_data_dag = DAG(
    dag_id="AMZN_data",
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=AMZN_data_dag,
    stock_name="AMZN",
    local_csv_path_template=AMZN_CSV_LOCAL_FILE_TEMPLATE,
    local_parquet_path_template_modfied=AMZN_PARQUET_LOCAL_FILE_TEMPLATE,
    tier1_gcs_path_template=AMZN_GCS_PATH_TEMPLATE_TIER1,
    tier2_gcs_path_template=AMZN_GCS_PATH_TEMPLATE_TIER2
)


AAPL_CSV_LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/data_APPLE.csv'
AAPL_PARQUET_LOCAL_FILE_TEMPLATE_MODIFIED = AIRFLOW_HOME + '/data_APPLE_modified.parquet'
AAPL_GCS_PATH_TEMPLATE_TIER1 = "tier1/APPLE/data_AAPLE.csv"
AAPL_GCS_PATH_TEMPLATE_TIER2 = "tier2/APPLE/data_AAPLE.parquet"

APPLE_data_dag = DAG(
    dag_id="APPLE_data",
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=APPLE_data_dag,
    stock_name="AAPL",
    local_csv_path_template=AAPL_CSV_LOCAL_FILE_TEMPLATE,
    local_parquet_path_template_modfied=AAPL_PARQUET_LOCAL_FILE_TEMPLATE_MODIFIED,
    tier1_gcs_path_template=AAPL_GCS_PATH_TEMPLATE_TIER1,
    tier2_gcs_path_template=AAPL_GCS_PATH_TEMPLATE_TIER2
)
