"""Alert callbacks for the flight price pipeline.

Airflow calls these functions automatically when a task is retried or fails,
and when a whole DAG run succeeds or fails. Each one writes a clearly marked
ALERT block to the Airflow logs, so problems are easy to spot: search the
logs for "ALERT".

Where to find them:
    Task alerts (retry, failure): in that task's log in the Airflow UI.
    DAG alerts (success, failure): in logs/dag_processor/<date>/dags-folder/
    flight_price_pipeline.py.log, because Airflow runs DAG-level callbacks
    in its DAG processor, not inside a task.
"""
import logging

log = logging.getLogger(__name__)


def _run_details(context, include_task=True):
    """Collect the facts every alert needs from Airflow's context.

    Args:
        context (dict): The context dictionary Airflow passes to callbacks.
        include_task (bool): Add the task name and attempt number. Use False
            for DAG-level alerts, which are about the whole run.

    Returns:
        str: A short multi-line description of the DAG, run (and task).
    """
    dag_run = context.get("dag_run")
    details = (
        f"  DAG:     {dag_run.dag_id if dag_run else 'unknown'}\n"
        f"  Run:     {dag_run.run_id if dag_run else 'unknown'}"
    )
    ti = context.get("ti")
    if include_task and ti:
        details += f"\n  Task:    {ti.task_id}\n  Attempt: {ti.try_number}"
    return details


def task_failure_alert(context):
    """Log an ERROR alert when a task fails for good (no retries left).

    Args:
        context (dict): The context dictionary Airflow passes to callbacks.
    """
    log.error(
        "🚨 ALERT: TASK FAILED\n%s\n  Error:   %s",
        _run_details(context),
        context.get("exception"),
    )


def task_retry_alert(context):
    """Log a WARNING alert when a task failed but Airflow will retry it.

    Args:
        context (dict): The context dictionary Airflow passes to callbacks.
    """
    log.warning(
        "⚠️ ALERT: TASK WILL BE RETRIED\n%s\n  Error:   %s",
        _run_details(context),
        context.get("exception"),
    )


def dag_success_alert(context):
    """Log an INFO message when every task in a DAG run has succeeded.

    Args:
        context (dict): The context dictionary Airflow passes to callbacks.
    """
    log.info("✅ ALERT: DAG RUN SUCCEEDED\n%s", _run_details(context, include_task=False))


def dag_failure_alert(context):
    """Log an ERROR alert when a DAG run ends in failure.

    Args:
        context (dict): The context dictionary Airflow passes to callbacks.
    """
    log.error(
        "🚨 ALERT: DAG RUN FAILED\n%s\n  Reason:  %s",
        _run_details(context, include_task=False),
        context.get("reason"),
    )