import os
import pendulum
from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.mailing.notifications_send_mail import notify_failure

# Environment-driven defaults for flexibility
default_email = os.getenv("ALERT_EMAIL", "syafrie@analyset.com")
default_retries = int(os.getenv("MAX_RETRIES", 3))
default_retry_delay = timedelta(minutes=int(os.getenv("RETRY_DELAY_MINUTES", 5)))

# Recommended: use timezone-aware pendulum for precise scheduling
start_date = pendulum.now("Asia/Jakarta").subtract(days=1)

default_args = {
    "owner": "airflow",                      # Task owner
    "depends_on_past": False,                # Independent task runs
    "start_date": start_date,                # Timezone-aware start date
    "email": [default_email],                # Failure notifications
    "email_on_failure": True,                # Send email on failure
    "email_on_retry": False,                 # Skip retry emails
    "retries": default_retries,              # Number of retries
    "retry_exponential_backoff": True,        # Use exponential backoff
    "max_retry_delay": timedelta(minutes=30),# Cap backoff delay
    "retry_delay": default_retry_delay,      # Initial retry delay
    "on_failure_callback": notify_failure,   # Custom failure handler
}
