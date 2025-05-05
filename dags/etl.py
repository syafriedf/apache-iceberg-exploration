from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from minio import Minio
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import os
from dotenv import load_dotenv
from datetime import timedelta

from plugins.utils import get_spark_session, get_last_success_time
from plugins.notifications import notify_failure

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['syafrie@analyset.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

# Initialize MinIO Client
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY", "minio_access_key"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minio_secret_key"),
    secure=False,
)

with DAG(
    dag_id='etl_bronze_silver_gold_final',
    default_args=default_args,
    description='ETL with Bronze, Silver, and Gold storage in MinIO, then loading Gold to ClickHouse',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'minio', 'postgres']
) as dag:

    @task()
    def extract(**context) -> str:
        spark = get_spark_session()
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

        last_run = get_last_success_time(context['dag'].dag_id, 'extract')
        sql = f"SELECT * FROM public.customer_data WHERE last_updated > '{last_run}'" ##SCDs Type 2 / Load only the latest data
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} rows from source table!")

        bronze_path = "/tmp/bronze_data.parquet"
        df.to_parquet(bronze_path, index=False)

        df_spark = spark.read.format('parquet').option('header', 'true').option('inferSchema', 'true').load(bronze_path)
        iceberg_table_name = "my_catalog.bronze.iceberg"

        df_spark.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(iceberg_table_name)

        logging.info("Bronze data uploaded to MinIO")

        iceberg_df = spark.read.format("iceberg").load(iceberg_table_name)
        iceberg_df.show()

        return bronze_path


    @task(dag=dag)
    def transform(bronze_path: str) -> str:
        """
        Read data from Bronze, perform transformations, and store it in the Silver Layer.
        """
        spark = get_spark_session()
        df = spark.read.format('parquet') \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .load(bronze_path)
        logging.info("Starting transformation process")
        
        # Example transformation: Remove duplicates
        df_cleaned = df.dropDuplicates()
        
        silver_path = "/tmp/silver_data.parquet"
        
        # Write cleaned DataFrame to Iceberg table for Silver Layer
        iceberg_table_name = "my_catalog.silver.iceberg"
        df_cleaned.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(iceberg_table_name)
        
        logging.info("Silver data uploaded to MinIO")
        
        # Read back the Iceberg table for verification
        iceberg_df = spark.table(iceberg_table_name)
        iceberg_df.show()
        
        return silver_path

    @task(dag=dag)
    def load(silver_path: str) -> str:
        """
        Use Silver data to create the Gold Layer (without additional aggregation)
        and upload the CSV file to MinIO.
        """
        spark = get_spark_session()
        df = spark.read.format('parquet') \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .load(silver_path)
        logging.info("Starting transformation process")
        
        # Example transformation: Remove duplicates
        # Perform aggregation by grouping by all columns and counting occurrences
        agg_df = df.groupBy("name", "email", "phone", "address") \
           .agg(F.count("*").alias("total_count"))
        
        agg_df.show(truncate=False)
        
        gold_path = "/tmp/gold_data.parquet"
        
        # Write cleaned DataFrame to Iceberg table for Gold Layer
        iceberg_table_name = "my_catalog.gold.iceberg"
        agg_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(iceberg_table_name)
        
        logging.info("Gold data uploaded to MinIO")
        
        # Read back the Iceberg table for verification
        iceberg_df = spark.table(iceberg_table_name)
        iceberg_df.show()
        
        return gold_path

    # Define task sequence
    bronze_layer = extract()
    silver_layer = transform(bronze_layer) 
    gold_layer = load(silver_layer) 

    bronze_layer >> silver_layer >> gold_layer
