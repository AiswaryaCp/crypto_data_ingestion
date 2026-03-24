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

        if not s3.check_for_key(file_path, bucket_name):
            raise FileNotFoundError(f"Missing file: s3://{bucket_name}/{file_path}")

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

        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            try:
                from sqlalchemy import text
                
                query = text(f"""
                    INSERT INTO {schema_name}.crypto_files (file_path) 
                    VALUES (:path)
                    ON CONFLICT (file_path) 
                    DO UPDATE SET 
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                """)
                
                result = conn.execute(query, {"path": file_path})
                file_id = result.fetchone()[0]

                df['file_id'] = file_id
                df = df.rename(columns={'id': 'crypto_id'})
                df.columns = [c.lower() for c in df.columns]

                conn.execute(text(f"DELETE FROM {schema_name}.crypto_data WHERE file_id = :fid"), {"fid": file_id})

                df.to_sql(
                    name='crypto_data',
                    schema=schema_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                print(f"Successfully loaded {len(df)} rows for file_id {file_id}")
            except Exception as e:
                print(f"Error during load: {e}")
                raise e