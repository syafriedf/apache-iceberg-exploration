import os
import pendulum
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf

from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.settings import Session

# Singleton holder for SparkSession\_instance
_spark_session = None

def get_spark_session(**kwargs):
    """
    Create or retrieve a singleton SparkSession with Iceberg configurations for MinIO.

    Configurable via environment variables:
      - SPARK_APP_NAME (default: ETL_Iceberg)
      - SPARK_MASTER_URL (default: local[*])
      - ICEBERG_CATALOG_NAME (default: my_catalog)
      - ICEBERG_WAREHOUSE (default: s3a://mybucket/)
      - S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
    """
    global _spark_session
    if _spark_session:
        return _spark_session

    # Read configs with defaults
    app_name = os.getenv('SPARK_APP_NAME', 'ETL_Iceberg')
    master_url = os.getenv('SPARK_MASTER_URL', 'local[*]')
    catalog = os.getenv('ICEBERG_CATALOG_NAME', 'my_catalog')
    warehouse = os.getenv('ICEBERG_WAREHOUSE', 's3a://mybucket/')
    s3_endpoint = os.getenv('S3_ENDPOINT_URL', 'http://minio:9000')
    s3_access = os.getenv('S3_ACCESS_KEY', 'minio_access_key')
    s3_secret = os.getenv('S3_SECRET_KEY', 'minio_secret_key')

    conf = (
        SparkConf()
        .setAppName(app_name)
        .setMaster(master_url)
        .set("spark.sql.catalog.%s" % catalog, "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.jars.packages",
             "org.apache.hadoop:hadoop-aws:3.3.4,"
             "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
             "org.apache.iceberg:iceberg-hive-runtime:1.5.0")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.%s.type" % catalog, "hadoop")
        .set("spark.sql.catalog.%s.warehouse" % catalog, warehouse)
        .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .set("spark.hadoop.fs.s3a.access.key", s3_access)
        .set("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
    )

    _spark_session = (
        SparkSession.builder.config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    logging.info(f"Initialized SparkSession {app_name} with Iceberg at {warehouse}")
    return _spark_session


def get_last_success_time(dag_id: str, task_id: str) -> pendulum.DateTime:
    """
    Retrieve the latest successful execution_date for a given DAG and task.

    Returns pendulum.DateTime in Asia/Jakarta, or epoch start if none.
    """
    session = Session()  # Airflow ORM session
    ti = (
        session.query(TaskInstance)
        .filter_by(dag_id=dag_id, task_id=task_id, state=State.SUCCESS)
        .order_by(TaskInstance.execution_date.desc())
        .first()
    )
    session.close()

    if ti and ti.execution_date:
        # Convert to timezone-aware pendulum
        return pendulum.instance(ti.execution_date, tz="Asia/Jakarta")
    # Fallback: epoch start
    return pendulum.datetime(1970, 1, 1, tz="Asia/Jakarta")
