"""DAG for daily check for new pokemon generations.

Executes .
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from degtyarev_util.control import check_for_new_generations


def _display(text: str) -> None:
    """Display supplied text. Obviously."""
    print(text)


with DAG(
    dag_id='degtyarev_daily_generations_check',
    start_date=days_ago(2),
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['degtyarev', 'de_school', 'pokemon']
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=_display,
        op_args=['Welcome to the world of tomorrow!']
    )

    check = PythonOperator(
        task_id='check_for_new_generations',
        python_callable=check_for_new_generations
    )

    success = PythonOperator(
        task_id='success',
        python_callable=_display,
        op_args=['SUCCESS'],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    fail = PythonOperator(
        task_id='failed',
        python_callable=_display,
        op_args=['FAIL'],
        trigger_rule=TriggerRule.ALL_FAILED
    )

    start >> check >> [success, fail]
