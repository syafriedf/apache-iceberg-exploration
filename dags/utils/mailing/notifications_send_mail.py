import os
import logging
from airflow.utils.email import send_email
import pendulum

def notify_failure(context):
    """
    Notify on DAG failure via email with detailed context information.

    Retries and recency controlled via environment variables:
    - ALERT_EMAILS: comma-separated list of recipient emails
    - DEFAULT_EMAIL: fallback if no ALERT_EMAILS provided
    """
    # Prepare email recipients
    env_emails = os.getenv("ALERT_EMAILS", "").split(",")
    default_email = os.getenv("DEFAULT_EMAIL", "syafrie@analyset.com")
    extra_email = 'syafriedwif@gmail.com'
    recipients = set(filter(None, [*env_emails, default_email, extra_email]))

    # Extract context details
    dag_id = context.get('dag').dag_id
    run_id = context.get('dag_run').run_id
    exec_date = context.get('execution_date')
    timestamp = pendulum.now("Asia/Jakarta").to_datetime_string()
    try_number = context.get('ti').try_number if context.get('ti') else 'N/A'
    log_url = context.get('task_instance').log_url if context.get('task_instance') else ''

    subject = f"[Airflow] DAG Failure: {dag_id} (Run: {run_id})"
    html_content = f"""
    <h3>Airflow DAG Failure Notification</h3>
    <ul>
      <li><strong>DAG:</strong> {dag_id}</li>
      <li><strong>Run ID:</strong> {run_id}</li>
      <li><strong>Execution Date:</strong> {exec_date}</li>
      <li><strong>Timestamp:</strong> {timestamp} (Asia/Jakarta)</li>
      <li><strong>Try Number:</strong> {try_number}</li>
      <li><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></li>
    </ul>
    """

    try:
        send_email(
            to=list(recipients),
            subject=subject,
            html_content=html_content
        )
        logging.info(f"Failure notification sent to: {recipients}")
    except Exception as e:
        logging.error(f"Failed to send failure notification: {e}")
