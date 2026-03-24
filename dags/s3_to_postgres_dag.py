import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(project_root)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.crypto_csv_parser import CryptoCSVParser

def run_parser_task(ds, **kwargs):
    bucket_name = os.getenv("BUCKET_NAME", "coin-crypto-data")
    root_folder = os.getenv("ROOT_FOLDER", "coingecko")

    parser = CryptoCSVParser(pg_conn_id='postgres_default')
    
    df, path = parser.get_s3_data(ds, bucket_name, root_folder)
    file_id = parser.write_to_files_table(path, bucket_name)
    parser.load_to_db(df, file_id, bucket_name)

with DAG(
    dag_id="s3_to_postgres_parser_v1",
    start_date=datetime(2026, 3, 1),
    schedule="45 10 * * *",
    catchup=False
) as dag:

    parse_and_load = PythonOperator(
        task_id="parse_s3_crypto_data",
        python_callable=run_parser_task
    )