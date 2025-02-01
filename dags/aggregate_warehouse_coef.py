from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import warehouse_coef.aggregate_files as wh_coef

DAG_COMMON_NAME = 'aggregate_warehouse_coef'
OWNER = 'Bezzubikov Kirill'
DAG_EMAIL = ['']
DEFAULT_POOL = 'default_pool'
MAX_ACTIVE_RUNS = 1
TAGS = ['wh_coef']

def create_dag():
    dag_id = f'{DAG_COMMON_NAME}'
    default_args = {
        'owner': OWNER,
        'email': DAG_EMAIL,
        'email_on_failure': True,
        'email_on_retry': False,
        'start_date': '2025-01-31',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        #'queue': queue,
        'max_active_runs': MAX_ACTIVE_RUNS,
        'concurrency': 1,
        'depends_on_past': False,
        'pool': DEFAULT_POOL
    }
    dag = DAG(
        dag_id = dag_id,
        catchup = True,
        #access_control = access_control,
        tags = TAGS,
        schedule_interval = '0 * * * *',
        #max_active_runs = max_active_runs,
        #concurrency = concurrency,
        default_args = default_args
    )
    aggregate_by_hour = PythonOperator(
        task_id = f'aggregate_files_by_hour',
        python_callable = wh_coef.aggregate_files,
        provide_context = True,
        op_kwargs = {'granularity_from': 'by_second', 'granularity_to': 'by_hour', 'current_datetime': '{{macros.datetime.strftime((execution_date + macros.timedelta(hours = 3)), "%Y-%m-%dT%H:%M:%S")}}'},
        dag = dag
    )
    aggregate_by_day = PythonOperator(
        task_id = f'aggregate_files_by_day',
        python_callable = wh_coef.aggregate_files,
        provide_context = True,
        op_kwargs = {'granularity_from': 'by_hour', 'granularity_to': 'by_day', 'current_datetime': '{{macros.datetime.strftime((execution_date + macros.timedelta(hours = 3)), "%Y-%m-%dT%H:%M:%S")}}'},
        dag = dag
    )
    prepare_mart = PythonOperator(
        task_id = f'prepare_mart',
        python_callable = wh_coef.prepare_mart,
        provide_context = True,
        op_kwargs = {'current_datetime': '{{macros.datetime.strftime((execution_date + macros.timedelta(hours = 3)), "%Y-%m-%dT%H:%M:%S")}}'},
        dag = dag
    )
    merge_marts = PythonOperator(
        task_id = f'merge_marts',
        python_callable = wh_coef.merge_marts,
        provide_context = True,
        op_kwargs = {
            'current_datetime': '{{macros.datetime.strftime((execution_date + macros.timedelta(hours = 3)), "%Y-%m-%dT%H:%M:%S")}}'
            , 'calc_depth_days': 18
        },
        dag = dag
    )

    aggregate_by_hour >> aggregate_by_day >> prepare_mart >> merge_marts
    return dag_id, dag

dag_id, dag = create_dag()
if dag is not None:
    globals()[dag_id] = dag