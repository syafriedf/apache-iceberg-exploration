from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.settings import Session

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

def get_last_success_time(dag_id, task_id):
    session = Session()
    ti = session.query(TaskInstance).filter_by(dag_id=dag_id, task_id=task_id, state=State.SUCCESS).order_by(TaskInstance.execution_date.desc()).first()
    return ti.execution_date if ti else datetime(1970, 1, 1)
