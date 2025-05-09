import os
import logging
from datetime import timedelta

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf
from minio import Minio

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.constants.constant import get_spark_session, get_last_success_time
from utils.constants.default_args import default_args

load_dotenv()

default_args.update({
    "start_date": days_ago(1)  # Ensure start_date is defined for scheduling
})

with DAG(
    dag_id='etl_bronze_silver_gold_final',
    default_args=default_args,
    description='ETL with Bronze, Silver, and Gold storage in MinIO',
    schedule_interval='0 2 * * *',  # Run daily at 2am
    catchup=False,
    tags=['DE_Team', 'critical', 'ingest', 'minutes'],
    dagrun_timeout=timedelta(minutes=15)  # Timeout entire DAG after 15 minutes
) as dag:

    @task(execution_timeout=timedelta(minutes=5))
    def extract(**context) -> str:
        spark = get_spark_session()
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')

        last_run = get_last_success_time(context['dag'].dag_id, 'extract')
        sql = f"SELECT * FROM public.customer_data WHERE last_updated > '{last_run}'"
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} rows from source table!")

        bronze_path = "/tmp/bronze_data.parquet"
        df.to_parquet(bronze_path, index=False)

        df_spark = spark.read.parquet(bronze_path)
        iceberg_table_name = "my_catalog.bronze.iceberg"

        df_spark.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(iceberg_table_name)

        logging.info("Bronze data uploaded to Iceberg")

        # cleanup
        os.remove(bronze_path)
        return bronze_path

    @task(execution_timeout=timedelta(minutes=5))
    def transform(bronze_path: str) -> str:
        spark = get_spark_session()
        df = spark.read.parquet(bronze_path)
        logging.info("Starting Silver transformation")
        
        df_cleaned = df.dropDuplicates()
        silver_path = "/tmp/silver_data.parquet"
        df_cleaned.write.parquet(silver_path, mode="overwrite")
        
        iceberg_table = "my_catalog.silver.iceberg"
        spark.read.parquet(silver_path).write.format("iceberg").mode("overwrite").saveAsTable(iceberg_table)
        logging.info("Silver data uploaded to Iceberg")

        # cleanup
        os.remove(bronze_path)
        return silver_path

    @task(execution_timeout=timedelta(minutes=5))
    def load(silver_path: str) -> str:  # No need to return path for final task
        spark = get_spark_session()
        df = spark.read.parquet(silver_path)
        logging.info("Starting Gold transformation")
        
        agg_df = df.groupBy("name", "email", "phone", "address").agg(F.count("*").alias("total_count"))
        gold_path = "/tmp/gold_data.parquet"
        agg_df.write.parquet(gold_path, mode="overwrite")

        iceberg_table = "my_catalog.gold.iceberg"
        spark.read.parquet(gold_path).write.format("iceberg").mode("overwrite").saveAsTable(iceberg_table)
        logging.info("Gold data uploaded to Iceberg")

        # cleanup
        os.remove(silver_path)
        return gold_path

   # Define task sequence
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task)

    extract_task >> transform_task >> load_task