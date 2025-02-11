#!/usr/bin/env bash
set -e

# Inisialisasi database Airflow
airflow db init

# Buat user admin jika belum ada (opsional)
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true

# Eksekusi perintah yang diteruskan ke container
exec "$@"
