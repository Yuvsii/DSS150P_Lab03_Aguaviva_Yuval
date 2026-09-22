from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

def failure_callback(context):
    print('==================== DAG FAILURE ====================')
    print(f"Task Failed: {context['task_instance'].task_id}")
    print(f"Run ID: {context['run_id']}")
    print(f"Execution Date: {context['execution_date']}")
    print('=====================================================')

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10), # Task B: Prevent hanging tasks
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )
    
    # Task B: Parameterized partition load
    load_cmd = (
        f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
        '{% if params.run_mode == "partition" %}'
        'python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}'
        '{% else %}'
        'python -m src.cli load'
        '{% endif %}'
    )
    load = BashOperator(
        task_id='load',
        bash_command=load_cmd,
    )
    
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate
