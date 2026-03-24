import os
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
    
    def write_to_files_table(self, file_path, bucket_name):
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        schema_name = bucket_name.replace('-', '_').lower()
        query = f"INSERT INTO {schema_name}.crypto_files (file_path) VALUES (%s) RETURNING id;"

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (file_path,))
                file_id = cursor.fetchone()
                if file_id:
                    file_id = file_id[0]
                    conn.commit()
                    print(f"DEBUG: Generated file_id is {file_id}")
                    return file_id
                else:
                    raise ValueError("Insert failed, no ID returned.")

    def load_to_db(self, df, file_id, bucket_name):
        df['file_id'] = file_id
        df = df.rename(columns={'id': 'crypto_id'})
        schema_name = bucket_name.replace('-', '_').lower()

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(
            name='crypto_data',
            schema=schema_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
