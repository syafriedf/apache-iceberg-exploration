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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize MinIO Client (using MinIO API port)
minio_client = Minio(
    "minio:9000",  # Use MinIO API port
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False,
)

def get_spark_session():
    """
    Create a SparkSession with Iceberg configuration so that tables will be stored
    in Apache Iceberg (Parquet) format in the warehouse located in MinIO.
    """
    conf = SparkConf().setAppName("ETL_Iceberg") \
        .setMaster("local[*]") \
        .set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .set("spark.sql.catalog.my_catalog.type", "hadoop") \
        .set("spark.sql.catalog.my_catalog.warehouse", "s3a://mybucket/") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .set("spark.hadoop.fs.s3a.access.key", "minio_access_key") \
        .set("spark.hadoop.fs.s3a.secret.key", "minio_secret_key") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .enableHiveSupport()
    return SparkSession.builder.config(conf=conf).getOrCreate()

iceberg_builder = SparkSession.builder \
    .master("local[*]") \
    .appName("iceberg-concurrent-write-isolation-test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://mybucket/") \
    .config("spark.hadoop.fs.s3a.access.key", "yminio_access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \


with DAG(
    dag_id='etl_bronze_silver_gold_final',
    default_args=default_args,
    description='ETL with Bronze, Silver, and Gold storage in MinIO, then loading Gold to ClickHouse',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['etl','minio','postgres']
) as dag:

    @task(dag=dag)
    def extract() -> str:
        
        spark = get_spark_session()

        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        sql = "SELECT * FROM public.customer_data"  # Sesuaikan dengan tabel customer_data
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} rows from source table!")

        bronze_path = "/tmp/bronze_data.parquet"
        df.to_parquet(bronze_path, index=False)

        df = spark.read.format('parquet').option('header', 'true').option('inferSchema', 'true').load(bronze_path)
        iceberg_table_name = "my_catalog.bronze.iceberg"
        # Write DataFrame to Iceberg table in Minio
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(iceberg_table_name)  # Name of the Iceberg table
        
        logging.info("Bronze data uploaded to MinIO")
        # # # Read data from the Iceberg table
        iceberg_df = spark.read.format("iceberg").load(iceberg_table_name)
        # # logging.info("Bronze data uploaded to MinIO")
        iceberg_df.show()
        # return bronze_path
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
