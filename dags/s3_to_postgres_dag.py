import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(project_root)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.crypto_csv_parser import CryptoCSVParser
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET = os.getenv("BUCKET_NAME", "coin-crypto-data")
ROOT_FOLDER = os.getenv("ROOT_FOLDER", "coingecko")
S3_FILE_PATH = f"{ROOT_FOLDER}/{{{{ ds }}}}/crypto_data.csv"

def run_parser_task(ds, **kwargs):
    parser = CryptoCSVParser(pg_conn_id='postgres_default')
    
    df, path = parser.get_s3_data(ds, bucket_name=BUCKET, root_folder=ROOT_FOLDER)
    parser.process_and_load(df, path, bucket_name=BUCKET)

with DAG(
    dag_id="s3_to_postgres_parser_v1",
    start_date=datetime(2026, 3, 1),
    schedule="45 10 * * *",
    catchup=False
) as dag:

    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=BUCKET,
        bucket_key=S3_FILE_PATH,
        aws_conn_id='aws_default',
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )

    parse_and_load = PythonOperator(
        task_id="parse_s3_crypto_data",
        python_callable=run_parser_task
    )

    wait_for_s3_file >> parse_and_load