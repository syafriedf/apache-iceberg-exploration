from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.mailing.notifications_send_mail import notify_failure

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