import traceback
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
import logging

logger = logging.getLogger(__name__)

def send_email_failure_alert(context):
    """
    Callback function that sends an email alert when an Airflow task fails.
    """
    try:
        task_instance: TaskInstance = context.get("task_instance")
        dag_run: DagRun = context.get("dag_run")
        exception = context.get("exception")
        
        exception_string = "No exception details available."
        if exception:
            try:
                formatted_tb = traceback.format_exception(type(exception), exception, exception.__traceback__)
                exception_string = f"<pre>{''.join(formatted_tb)}</pre>"
            except Exception:
                exception_string = f"<pre>{str(exception)}</pre>"
            
        subject = f"FAILURE: {dag_run.dag_id} - {task_instance.task_id}"
        
        html_content = f"""
        <h3>Airflow Task Failure</h3>
        <ul>
            <li><b>DAG:</b> {dag_run.dag_id}</li>
            <li><b>Task:</b> {task_instance.task_id}</li>
            <li><b>Run ID:</b> {dag_run.run_id}</li>
            <li><b>Time:</b> {dag_run.logical_date}</li>
        </ul>
        <p><b>Log URL:</b> <a href="{task_instance.log_url}">View Logs</a></p>
        <hr>
        <h4>Error Details:</h4>
        {exception_string}
        """
        
        email_op = EmailOperator(
            task_id="send_failure_email",
            to=["ojokayode13@gmail.com"],
            subject=subject,
            html_content=html_content,
            conn_id="smtp_conn" 
        )
        
        email_op.execute(context=context)
        logger.info(f"Sent failure email for {dag_run.dag_id}")

    except Exception as e:
        logger.error(f"Failed to send failure email: {e}")
        traceback.print_exc()