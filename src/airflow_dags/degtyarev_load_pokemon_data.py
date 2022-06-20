"""Module with Airflow DAG that controls loading pokemon data to S3."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from degtyarev_util.control import display, extract_and_save_data

with DAG(
    dag_id='degtyarev_load_pokemon_data',
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['degtyarev', 'de_school', 'pokemon']
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=display,
        op_args=['Load data DAG started.']
    )

    extract_and_load = PythonOperator(
        task_id='extract_and_save_data',
        python_callable=extract_and_save_data
    )

    success = PythonOperator(
        task_id='success',
        python_callable=display,
        op_args=['SUCCESS.'],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    fail = PythonOperator(
        task_id='failed',
        python_callable=display,
        op_args=['FAIL'],
        trigger_rule=TriggerRule.ALL_FAILED
    )

    start >> extract_and_load >> [success, fail]
