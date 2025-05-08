from airflow.utils.email import send_email

def notify_failure(context):
    send_email(
        to=context['params'].get('email', 'syafrie@analyset.com'),
        subject=f"DAG Failed: {context['dag'].dag_id}",
        html_content=f"<h3>DAG {context['dag'].dag_id} has failed.</h3><p>Run ID: {context['dag_run'].run_id}</p>"
    )
