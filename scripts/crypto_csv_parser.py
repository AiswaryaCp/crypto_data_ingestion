import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class CryptoCSVParser:
    def __init__(self, pg_conn_id='postgres_default', aws_conn_id='aws_default'):
        self.pg_conn_id = pg_conn_id
        self.aws_conn_id = aws_conn_id
    
    def get_s3_data(self, ds, bucket_name, root_folder):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        file_path = f"{root_folder}/{ds}/crypto_data.csv"
        s3_obj = s3.get_key(
            key=file_path,
            bucket_name=bucket_name
        )
        
        file_content = s3_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(file_content))
        print("DataFrame shape")
        print(df.head())
        return df, file_path
    
    def process_and_load(self, df, file_path, bucket_name):
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        schema_name = bucket_name.replace('-', '_').lower()

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            query = f"INSERT INTO {schema_name}.crypto_files (file_path) VALUES (%s) RETURNING id;"
            cursor.execute(query, (file_path,))
            file_id = cursor.fetchone()[0]

            df['file_id'] = file_id
            df = df.rename(columns={'id':'crypo_id'})

            data_to_insert = [tuple(x) for x in df.values]
            target_fields = list(df.columns)

            pg_hook.insert_rows(
                table=f"{schema_name}.crypto_data",
                rows=data_to_insert,
                target_fields=target_fields,
                commit_every=0
            )

            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()