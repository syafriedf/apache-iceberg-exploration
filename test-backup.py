from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from minio import Minio
import pandas as pd
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Inisialisasi MinIO Client (menggunakan port API MinIO)
minio_client = Minio(
    "minio:9000",  # Gunakan port API MinIO
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False,
)

with DAG(
    dag_id='etl_bronze_silver_gold_new',
    default_args=default_args,
    description='ETL dengan penyimpanan Bronze, Silver, dan Gold di MinIO, kemudian memuat Gold ke ClickHouse',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['etl','minio','postgres']
) as dag:

    @task(dag=dag)
    def extract() -> str:
        """
        Mengambil data dari PostgreSQL, menyimpannya ke MinIO (Bronze Layer).
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        sql = "SELECT * FROM public.customer_data"  # Sesuaikan dengan tabel customer_data
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} rows from source table")

        bronze_path = "/tmp/bronze_data.csv"
        df.to_csv(bronze_path, index=False)

        # Unggah file Bronze ke MinIO
        minio_client.fput_object(
            bucket_name="mybucket",
            object_name="bronze/bronze_data.csv",
            file_path=bronze_path,
            content_type="application/csv"
        )
        logging.info("Bronze data uploaded to MinIO")
        return bronze_path

    @task(dag=dag)
    def transform(bronze_path: str) -> str:
        """
        Membaca data dari Bronze, melakukan transformasi, dan menyimpannya ke Silver Layer.
        """
        df = pd.read_csv(bronze_path)
        logging.info("Starting transformation process")

        # Contoh transformasi: Hapus duplikat
        df_cleaned = df.drop_duplicates()

        silver_path = "/tmp/silver_data.csv"
        df_cleaned.to_csv(silver_path, index=False)

                # # Karena output Parquet adalah direktori, cari file Parquet di dalam direktori tersebut
        # file_list = os.listdir(silver_path)
        # parquet_files = [f for f in file_list if f.endswith(".parquet")]
        # if not parquet_files:
        #     raise Exception("Tidak ditemukan file Parquet dalam direktori Silver.")
        # # Ambil file pertama yang ditemukan
        # single_parquet_file = os.path.join(silver_path, parquet_files[0])
        
        # # Unggah file Silver ke MinIO
        # minio_client.fput_object(
        #     bucket_name="mybucket",
        #     object_name="silver/silver_data.parquet",
        #     file_path=single_parquet_file,
        #     content_type="application/parquet"
        # )

        # Unggah file Silver ke MinIO
        minio_client.fput_object(
            bucket_name="mybucket",
            object_name="silver/silver_data.csv",
            file_path=silver_path,
            content_type="application/csv"
        )
        logging.info("Silver data uploaded to MinIO")
        return silver_path

    @task(dag=dag)
    def load(silver_path: str) -> str:
        """
        Menggunakan data Silver untuk membentuk Gold Layer (tanpa agregasi tambahan)
        dan mengunggah file CSV ke MinIO.
        """
        df = pd.read_csv(silver_path)
        logging.info("Starting load process for Gold layer")

        # Karena tidak ada agregasi khusus, data Gold sama dengan data Silver
        gold_path = "/tmp/gold_data.csv"
        df.to_csv(gold_path, index=False)

        # Unggah file Gold ke MinIO
        minio_client.fput_object(
            bucket_name="mybucket",
            object_name="gold/gold_data.csv",
            file_path=gold_path,
            content_type="application/csv"
        )
        logging.info("Gold data uploaded to MinIO")
        return gold_path

    # Definisikan urutan tugas
    bronze_csv = extract()
    silver_csv = transform(bronze_csv) 
    gold_csv = load(silver_csv) 

    bronze_csv >> silver_csv >> gold_csv
